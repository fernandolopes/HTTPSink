package io.github.fernandolopes.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);
	public static Timeout getTimeout(String currentTime) {

		Pattern pattner = Pattern.compile("^([\\d]*)\\s([a-zA-Z]*)$");
		Matcher matcher = pattner.matcher(currentTime);
		Timeout time = null;
		try {
			if (matcher.find()) {
				switch (matcher.group(2)) {
				case "minutes":
					time = Timeout.ofMinutes(Integer.valueOf(matcher.group(1)));
					break;
				case "seconds":
					time = Timeout.ofSeconds(Integer.valueOf(matcher.group(1)));
					break;
				case "milliseconds":
					time = Timeout.ofMilliseconds(Integer.valueOf(matcher.group(1)));
					break;
				default:
					time = Timeout.ofMilliseconds(0);
					break;
				}
			} else {
				time = Timeout.ofMilliseconds(Integer.valueOf(currentTime));
			}
		} catch (Exception e) {
			time = Timeout.ofMilliseconds(0);
		}
		
		return time;
	}
	
	public static String replaceRequestUri(String uri, String key, String topics, String output, Object record) throws Exception {
	    // Substituir ${topic} com o valor de topics
	    String uriWithTopics = uri.replaceAll("\\$\\{topic\\}", topics);
	    
	    // Substituir ${key} com o valor de key, se key não for nulo
	    if (key != null) {
	        uriWithTopics = uriWithTopics.replaceAll("\\$\\{key\\}", key.replace("\"", ""));
	    }
	    
	    // Verificar se o output é "json" e se existem placeholders a serem substituídos
        if (output.equals("json")) {
            Pattern pattern = Pattern.compile("\\$\\{([a-zA-Z0-9_]+)\\}", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(uriWithTopics);

            StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                String keySearch = matcher.group(1);
                String value = extractValueFromRecord(record, keySearch);
                matcher.appendReplacement(sb, value != null ? Matcher.quoteReplacement(value) : "");
            }
            matcher.appendTail(sb);
            uriWithTopics = sb.toString();
        }
        log.info("url: " + uriWithTopics);
	    return uriWithTopics;
	}
	
	public static InputStream convertToInputStream(HashMap<String, Object> map) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(map);
            
            return new ByteArrayInputStream(jsonString.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
	
	public static String[] extractIds(String traceparent) {
        // Regex pattern to match traceparent format
        String pattern = "^(\\w{2})-(\\w{32})-(\\w{16})-(\\w{2})?$";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(traceparent);
        
        String traceId = "";
        String spanId = "";

        if (m.find()) {
            traceId = m.group(2);
            spanId = m.group(3);
        }
        
        return new String[] { traceId, spanId };
    }
	
	// Método para extrair o valor do registro (record) baseado na chave (key)
	private static String extractValueFromRecord(Object record, String key) {
	    try {
	        if (record instanceof String) {
	            JSONObject json = new JSONObject((String) record);
	            return json.optString(key, null);
	        } else if (record instanceof HashMap) {
	            @SuppressWarnings("unchecked")
	            HashMap<String, Object> map = (HashMap<String, Object>) record;
	            Object value = map.get(key);
	            return value != null ? value.toString() : null;
	        } else if (record instanceof Struct) {
	            Struct struct = (Struct) record;
	            try {
	                // Verificar se o campo existe no schema
	                if (struct.schema().field(key) != null) {
	                    Object value = struct.get(key);
	                    return value != null ? value.toString() : null;
	                } else {
	                    log.warn("Campo '{}' não encontrado no schema da Struct", key);
	                    return null;
	                }
	            } catch (Exception e) {
	                log.error("Erro ao extrair campo '{}' da Struct: {}", key, e.getMessage());
	                return null;
	            }
	        } else if (record instanceof JSONObject) {
	            JSONObject json = (JSONObject) record;
	            return json.optString(key, null);
	        } else {
	            log.warn("Tipo de record não suportado: {}. Tentando converter para String.", record.getClass().getSimpleName());
	            return record.toString();
	        }
	    } catch (Exception e) {
	        log.error("Erro geral ao extrair valor do record para chave '{}': {}", key, e.getMessage());
	        return null;
	    }
	}
}
