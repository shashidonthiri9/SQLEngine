package db_;

public class Payload {
	
	
	byte numberOfColumns;
	byte[] dataType;
	String[] data;
	
	
	public byte getNumberOfColumns() {
		return numberOfColumns;
	}
	public void setNumberOfColumns(byte numberOfColumns) {
		this.numberOfColumns = numberOfColumns;
	}
	public byte[] getDataType() {
		return dataType;
	}
	public void setDataType(byte[] dataType) {
		this.dataType = dataType;
	}
	public String[] getData() {
		return data;
	}
	public void setData(String[] data) {
		this.data = data;
	}
	
	

}
