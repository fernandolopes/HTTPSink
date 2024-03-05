package br.com.fernandolopez.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hc.core5.util.Timeout;

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
}
