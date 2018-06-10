package db_;

public class Cell {
	
	int pageNumber;
	short payLoadSize;
	int rowId;
	Payload payload;
	short location;
	
	public short getCellLocation() {
		return location;
	}
	public void setCellLocation(short location) {
		this.location = location;
	}
	public int Get_page_number() {
		return pageNumber;
	}
	public void Set_page_number(int pageNumber) {
		this.pageNumber = pageNumber;
	}

	
	public short Get_payload_Size() {
		return payLoadSize;
	}
	public void Set_Payload_Size(short payLoadSize) {
		this.payLoadSize = payLoadSize;
	}
	public int getRowId() {
		return rowId;
	}
	public void setRowId(int rowId) {
		this.rowId = rowId;
	}
	public Payload Get_Payload() {
		return payload;
	}
	public void set_Payload(Payload payload) {
		this.payload = payload;
	}
	
	

}
