import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public class FullETLGenerator {
    private static final String ARQUIVO_FONTE = "dados/siga-empreendimentos-geracao.csv";
    private static final String DIM_GERACAO_FILE = "dim_geracao.csv";
    private static final String DIM_STATUS_FILE = "dim_status.csv";
    private static final String DIM_LOCALIZACAO_FILE = "dim_localizacao.csv";
    private static final String DIM_EMPREENDIMENTO_FILE = "dim_empreendimento.csv";
    private static final String DIM_TEMPO_FILE = "dim_tempo.csv";
    private static final String FATO_OUTPUT_FILE = "fato_geracao.csv";
    private static Map<String, Integer> geracaoIdMap = new HashMap<>();
    private static Map<String, Integer> statusIdMap = new HashMap<>();
    private static Map<String, Integer> localizacaoIdMap = new HashMap<>();
    private static Map<String, CSVRecord> empreendimentoMap = new HashMap<>(); 
    private static final CSVFormat CSV_FORMAT_LEITURA = CSVFormat.DEFAULT.builder()
            .setDelimiter(';')
            .setHeader()
            .setTrim(true)
            .build();      
    private static final CSVFormat CSV_FORMAT_ESCRITA = CSVFormat.DEFAULT.builder()
            .setDelimiter(';')
            .build();
    public static void main(String[] args) {
        try {
            System.out.println("--- PROCESSO DE ETL INICIADO ---");
            System.out.println("\nPASSO 1: Gerando Dimensões...");
            passo1_GerarDimensoes();
            System.out.println("...Dimensões geradas com sucesso.");
            System.out.println("\nPASSO 2: Gerando Tabela Fato...");
            passo2_GerarFato();
            System.out.println("...Tabela Fato gerada com sucesso.");
            System.out.println("\n--- PROCESSO DE ETL CONCLUÍDO ---");
            System.out.println("Arquivos gerados na pasta do projeto.");

        } catch (IOException e) {
            System.err.println("Ocorreu um erro fatal no ETL:");
            e.printStackTrace();
        }
    }
    private static void passo1_GerarDimensoes() throws IOException {
        LocalDate dataMinima = LocalDate.MAX;
        LocalDate dataMaxima = LocalDate.MIN;

        try (
            Reader reader = Files.newBufferedReader(Paths.get(ARQUIVO_FONTE), StandardCharsets.ISO_8859_1);
            CSVParser csvParser = new CSVParser(reader, CSV_FORMAT_LEITURA);
            CSVPrinter printerGeracao = new CSVPrinter(Files.newBufferedWriter(Paths.get(DIM_GERACAO_FILE), StandardCharsets.ISO_8859_1), CSV_FORMAT_ESCRITA);
            CSVPrinter printerStatus = new CSVPrinter(Files.newBufferedWriter(Paths.get(DIM_STATUS_FILE), StandardCharsets.ISO_8859_1), CSV_FORMAT_ESCRITA);
            CSVPrinter printerLocal = new CSVPrinter(Files.newBufferedWriter(Paths.get(DIM_LOCALIZACAO_FILE), StandardCharsets.ISO_8859_1), CSV_FORMAT_ESCRITA);
            CSVPrinter printerEmp = new CSVPrinter(Files.newBufferedWriter(Paths.get(DIM_EMPREENDIMENTO_FILE), StandardCharsets.ISO_8859_1), CSV_FORMAT_ESCRITA)
        ) {
            printerGeracao.printRecord("ID_Geracao", "SigTipoGeracao", "DscOrigemCombustivel", "DscFonteCombustivel");
            printerStatus.printRecord("ID_Status", "DscFaseUsina", "DscTipoOutorga", "IdcGeracaoQualificada");
            printerLocal.printRecord("ID_Localizacao", "SigUFPrincipal", "DscMuninicpios");
            printerEmp.printRecord("CodCEG", "NomEmpreendimento", "DscPropriRegimePariticipacao");
            int idGeracao = 1;
            int idStatus = 1;
            int idLocal = 1;
            
            System.out.println("...lendo arquivo fonte e descobrindo dimensões...");

            for (CSVRecord record : csvParser) {
                String keyGeracao = buildGeracaoKey(record);
                if (!geracaoIdMap.containsKey(keyGeracao)) {
                    geracaoIdMap.put(keyGeracao, idGeracao);
                    printerGeracao.printRecord(
                        idGeracao,
                        record.get("SigTipoGeracao"),
                        record.get("DscOrigemCombustivel"),
                        record.get("DscFonteCombustivel")
                    );
                    idGeracao++;
                }

                String keyStatus = buildStatusKey(record);
                if (!statusIdMap.containsKey(keyStatus)) {
                    statusIdMap.put(keyStatus, idStatus);
                    printerStatus.printRecord(
                        idStatus,
                        record.get("DscFaseUsina"),
                        record.get("DscTipoOutorga"),
                        getGeracaoQualificada(record)
                    );
                    idStatus++;
                }
                
                String keyLocal = buildLocalizacaoKey(record);
                if (!localizacaoIdMap.containsKey(keyLocal)) {
                    localizacaoIdMap.put(keyLocal, idLocal);
                    printerLocal.printRecord(
                        idLocal,
                        record.get("SigUFPrincipal"),
                        record.get("DscMuninicpios")
                    );
                    idLocal++;
                }

                String keyEmp = record.get("CodCEG");
                if (!empreendimentoMap.containsKey(keyEmp)) {
                    empreendimentoMap.put(keyEmp, record); 
                    printerEmp.printRecord(
                        keyEmp,
                        record.get("NomEmpreendimento"),
                        record.get("DscPropriRegimePariticipacao")
                    );
                }

                LocalDate dataOp = parseDate(record.get("DatEntradaOperacao"));
                if (dataOp != null) {
                    if (dataOp.isBefore(dataMinima)) dataMinima = dataOp;
                    if (dataOp.isAfter(dataMaxima)) dataMaxima = dataOp;
                }
            }
        } 

        if (dataMinima != LocalDate.MAX) {
            System.out.println("...gerando Dim_Tempo de " + dataMinima + " até " + dataMaxima);
            gerarDimTempo(dataMinima, dataMaxima);
        } else {
            System.out.println("...nenhuma data válida encontrada para Dim_Tempo.");
        }
    }

    private static void passo2_GerarFato() throws IOException {
        try (
            Reader reader = Files.newBufferedReader(Paths.get(ARQUIVO_FONTE), StandardCharsets.ISO_8859_1);
            CSVParser csvParser = new CSVParser(reader, CSV_FORMAT_LEITURA);

            CSVPrinter printerFato = new CSVPrinter(Files.newBufferedWriter(Paths.get(FATO_OUTPUT_FILE), StandardCharsets.ISO_8859_1), CSV_FORMAT_ESCRITA)
        ) {
            printerFato.printRecord(
                "ID_Geracao", "ID_Status", "ID_Localizacao", "CodCEG", "FK_DataOperacao",
                "MdaPotenciaOutorgadaKw", "MdaPotenciaFiscalizadaKw", "MdaGarantiaFisicaKw", "QtdEmpreendimentos"
            );

            for (CSVRecord record : csvParser) {
                String keyGeracao = buildGeracaoKey(record);
                Integer idGeracao = geracaoIdMap.get(keyGeracao);

                String keyStatus = buildStatusKey(record);
                Integer idStatus = statusIdMap.get(keyStatus);
                String keyLocal = buildLocalizacaoKey(record);
                Integer idLocalizacao = localizacaoIdMap.get(keyLocal);

                String codCEG = record.get("CodCEG");
                int fkData = parseDateKey(record.get("DatEntradaOperacao"));
                double potOutorgada = parseDouble(record.get("MdaPotenciaOutorgadaKw"));
                double potFiscalizada = parseDouble(record.get("MdaPotenciaFiscalizadaKw"));
                double potGarantia = parseDouble(record.get("MdaGarantiaFisicaKw"));

                printerFato.printRecord(
                    (idGeracao != null) ? idGeracao : -1,
                    (idStatus != null) ? idStatus : -1,
                    (idLocalizacao != null) ? idLocalizacao : -1,
                    codCEG,
                    fkData,
                    String.format(Locale.GERMAN, "%.2f", potOutorgada), 
                    String.format(Locale.GERMAN, "%.2f", potFiscalizada),
                    String.format(Locale.GERMAN, "%.2f", potGarantia),
                    1
                );
            }
        }
    }
    
    private static void gerarDimTempo(LocalDate dataInicio, LocalDate dataFim) throws IOException {
        Locale localeBR = new Locale("pt", "BR");

        try (CSVPrinter printerTempo = new CSVPrinter(Files.newBufferedWriter(Paths.get(DIM_TEMPO_FILE), StandardCharsets.ISO_8859_1), CSV_FORMAT_ESCRITA)) {
            printerTempo.printRecord("ChaveData", "DataCompleta", "Ano", "MesNumero", "NomeMes", "Dia", "DiaDaSemana", "Trimestre");
            
            for (LocalDate data = dataInicio; !data.isAfter(dataFim); data = data.plusDays(1)) {
                int chaveData = Integer.parseInt(data.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
                int trimestre = (data.getMonthValue() - 1) / 3 + 1;
                
                printerTempo.printRecord(
                    chaveData,
                    data.format(DateTimeFormatter.ISO_LOCAL_DATE),
                    data.getYear(),
                    data.getMonthValue(),
                    data.getMonth().getDisplayName(TextStyle.FULL, localeBR),
                    data.getDayOfMonth(),
                    data.getDayOfWeek().getDisplayName(TextStyle.FULL, localeBR),
                    "T" + trimestre
                );
            }
        }
    }

    private static String getGeracaoQualificada(CSVRecord record) {
        String qualificada = record.get("IdcGeracaoQualificada");
        return (qualificada == null || qualificada.isEmpty()) ? "N/A" : qualificada;
    }
    
    private static String coordComPonto(String coord) {
        return coord.replace(",", ".");
    }
    
    private static String buildGeracaoKey(CSVRecord record) {
        return record.get("SigTipoGeracao") + ";" + 
               record.get("DscOrigemCombustivel") + ";" + 
               record.get("DscFonteCombustivel");
    }

    private static String buildStatusKey(CSVRecord record) {
        return record.get("DscFaseUsina") + ";" + 
               record.get("DscTipoOutorga") + ";" + 
               getGeracaoQualificada(record);
    }

    private static String buildLocalizacaoKey(CSVRecord record) {
        return record.get("SigUFPrincipal") + ";" + 
               record.get("DscMuninicpios");
    }

    private static double parseDouble(String value) {
        if (value == null || value.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(value.replace(".", "").replace(",", "."));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static LocalDate parseDate(String dateString) {
        if (dateString == null || dateString.isEmpty() || dateString.length() < 10) return null;
        try {
            return LocalDate.parse(dateString.substring(0, 10));
        } catch (Exception e) {
            return null; 
        }
    }
    
    private static int parseDateKey(String dateString) {
        LocalDate data = parseDate(dateString);
        if (data == null) return 0; 
        return Integer.parseInt(data.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    }
}
