package br.com.fernandolopez.core;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hc.core5.util.Timeout;
import org.json.JSONObject;

public class Utils {

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
	
//	public static String replaceRequestUri(String uri, String key, String topics, String output, Object record) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
//		
//		var uriA = uri.replaceAll("(\\$\\{topic\\})", topics);
//		var uriB = (key != null)? uriA.replaceAll("(\\$\\{key\\})", key) : uriA;
//		
//		System.out.println("data: " + record.getClass());
//		if (output.equals("json")) {
//			Pattern pattern = Pattern.compile("(\\$\\{[a-zA-Z]+\\})", Pattern.CASE_INSENSITIVE);
//		    Matcher matcher = pattern.matcher(uriB);
//		    boolean matchFound = matcher.find();
//		    
//		    if(matchFound) {
//		    	var keySearch = matcher.group(1).replace("${", "").replace("}", "");
//		    	String value = null;
//		    	if (record.getClass().toString().equals("class java.lang.String")) {
//			    	var json = new JSONObject(record.toString());
//			    	value = json.getString(keySearch);
//		    	} else {
//		    		var json = (HashMap<?, ?>)record;
//		    		value = json.get(keySearch).toString();
//	    		    
//		    	}
//		    	return uriA.replaceAll("(\\$\\{"+keySearch+"\\})", value);
//		    }
//			
//		}
//		
//		
//		return uriB;
//	}
	
	public static String replaceRequestUri(String uri, String key, String topics, String output, Object record) throws Exception {
	    // Substituir ${topic} com o valor de topics
	    String uriWithTopics = uri.replaceAll("\\$\\{topic\\}", topics);
	    
	    // Substituir ${key} com o valor de key, se key não for nulo
	    if (key != null) {
	        uriWithTopics = uriWithTopics.replaceAll("\\$\\{key\\}", key);
	    }
	    
	    // Verificar se o output é "json" e se existem placeholders a serem substituídos
	    if (output.equals("json")) {
	        Pattern pattern = Pattern.compile("\\$\\{[a-zA-Z]+\\}", Pattern.CASE_INSENSITIVE);
	        Matcher matcher = pattern.matcher(uriWithTopics);

	        // Iterar pelos placeholders encontrados na URI
	        while (matcher.find()) {
	            String placeholder = matcher.group();
	            String keySearch = placeholder.replaceAll("\\$\\{|\\}", "");
	            String value = extractValueFromRecord(record, keySearch);
	            uriWithTopics = uriWithTopics.replaceAll(Pattern.quote(placeholder), value != null ? Matcher.quoteReplacement(value) : "");
	        }
	    }
	    
	    return uriWithTopics;
	}
	
	// Método para extrair o valor do registro (record) baseado na chave (key)
	private static String extractValueFromRecord(Object record, String key) {
	    if (record instanceof String) {
	        JSONObject json = new JSONObject((String) record);
	        return json.optString(key);
	    } else if (record instanceof HashMap) {
	        @SuppressWarnings("unchecked")
	        HashMap<String, Object> map = (HashMap<String, Object>) record;
	        Object value = map.get(key);
	        return value != null ? value.toString() : null;
	    }
	    return null;
	}
	
	
}
