package db_;

import java.util.Map;

public class Page {
	
	
	public byte Get_Page_Type() {
		return pageType;
	}
	public void Set_Page_Type(byte pageType) {
		this.pageType = pageType;
	}
	int pageNo;
	byte pageType;
	
	Map<Integer,Cell> records;
	
	public int Get_page_number() {
		return pageNo;
	}
	public void Set_page_number(int pageNo) {
		this.pageNo = pageNo;
	}
	public Map<Integer, Cell> getRecords() {
		return records;
	}
	public void Set_Records(Map<Integer, Cell> records) {
		this.records = records;
	}

}
