package com.bizosys.oneline.maintenance;

import java.util.HashMap;
import java.util.Map;

public class CommandLineArguments {
	
	Map<String, String> allArgs = new HashMap<String, String>(16); 
	
	public CommandLineArguments(String[] args) {
		for (String arg : args) {
			String[] nv = StringUtils.getStrings(arg, '=');
			if ( nv.length != 2) continue;
			allArgs.put(nv[0], nv[1]);
		}
	}
	
	public String get(String name) {
		return allArgs.get(name);
	}
	
	public void print() {
		System.out.println("Command Line Arguments");
		System.out.println("========================");
		for (String key : allArgs.keySet()) {
			System.out.println("[" + key + "]\t=\t[" + allArgs.get(key) + "]");
		}
	}
}
