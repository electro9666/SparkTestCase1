package org.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;

public class LogUtil {

	List<String> currentFile = null;
	
	public boolean isDebug = true;
	private int stackTraceDepth;
	
	public void setDebug(boolean isDebug){
		this.isDebug = isDebug;
	}
	public LogUtil(boolean isDebug, int stackTraceDepth, String filePath){
		this.isDebug = isDebug;
		this.stackTraceDepth = stackTraceDepth;
		
		//log4j 설정(로그레벨 WARN)... 일단 경로는 하드코딩-_-;;
		PropertyConfigurator.configure("D:\\workspace\\spark\\learning.spark\\src\\resources\\log4j.properties");
		
		if(currentFile == null){
			currentFile = new ArrayList<String>();
			BufferedReader br = null;
			try {
			
				br = new BufferedReader(new FileReader(filePath));
				String temp = null;
				currentFile.add("");	//0번째 자리 빈값으로...
				while((temp = br.readLine()) != null){
					int index = temp.indexOf("=");
					if(index != -1){
						currentFile.add(temp.substring(0, index+1).trim() + " ");
					}else{
						currentFile.add("");
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally{
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * 변수에 값을 할당할때, 사용하면, 일단 화면에 한번 출력해주고, 할당한다.
	 * StackTraceElement를 사용하면, 어느 라인에서 할당했는지 알 수 있음.
	 * 디버그용으로 좋음!!
	 */
	public <T> T s(T o){	
		if(isDebug){
			try{
				Thread th = Thread.currentThread();
				StackTraceElement[] lists = th.getStackTrace();
	//			int lineNumber = lists[2].getLineNumber();
				int lineNumber = lists[this.stackTraceDepth].getLineNumber();
				String variable = currentFile.get(lineNumber);
				System.out.print("\n" +  + lineNumber + ": " + variable + o);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		return o;
	}
}
