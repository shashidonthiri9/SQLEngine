package db_;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

public class Operations implements Constants {
	
	
	public static void Insert(String tableName, String[] values) {

		try {
			tableName=tableName.trim();
			String path="data/userdata/"+tableName+".tbl";
			if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path="data/catalog/"+tableName+".tbl";
			
			
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			//int noOfPages = (int) (table.length() / PAGESIZE);
			//Map<Integer, String> colNames = getColumnNames(tableName);
			String condition[] = { "table_name", "=", tableName };
			String columnNames[] = { "*" };
			Map<Integer, Cell> columnsMeta = getColumnsMeta(tableName, columnNames, condition);
			String[] dataType = getDataType(columnsMeta);
			String[] isNullable = isNullable(columnsMeta);

			for (int i = 0; i < values.length; i++) {
				if (values[i].equalsIgnoreCase("null") && isNullable[i].equals("NO")) {
					System.out.println("Cannot insert NULL values in NOT NULL field");
					return;
				}
			}
			condition = new String[0];
			
			//get page number on which data exist
			int pageNo=getPageNo(tableName,Integer.parseInt(values[0]));
			
			
			//check for duplicate value
			Map<Integer, Cell> data = getData(tableName, columnNames, condition);
			if (data.containsKey(Integer.parseInt(values[0]))) {
				System.out.println("Duplicate value for primary key");
				return;
			}

			// check leaf size
			byte[] plDataType = new byte[dataType.length - 1];
			int payLoadSize = Get_payload_Size(tableName, values, plDataType, dataType);
			payLoadSize=payLoadSize+6;
			
			//change offset calculation??
			int address=checkPageOverFlow(table,pageNo,payLoadSize);
			
			if(address!=-1){
				Cell cell=createCell(pageNo,Integer.parseInt(values[0]),(short)payLoadSize,plDataType,values);
				writePayload(table,cell,address);
			}
			else
			{
				Split_leaf_PAGE(table,pageNo);
				int pNo=getPageNo(tableName,Integer.parseInt(values[0]));
				int addr=checkPageOverFlow(table,pNo,payLoadSize);
				Cell cell=createCell(pNo,Integer.parseInt(values[0]),(short)payLoadSize,plDataType,values);
				writePayload(table,cell,addr);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private static void Split_leaf_PAGE(RandomAccessFile table,int currentPage) {
		// TODO Auto-generated method stub
		int newPage=createNewPage(table);
		int midKey=divideData(table,currentPage);
		moveRecords(table,currentPage,newPage,midKey);
		
	}

	
	private static void moveRecords(RandomAccessFile table,int currentPage,int newPage,int midKey) {
		// TODO Auto-generated method stub
		try{

			table.seek((currentPage)*PAGESIZE);
			byte pageType = table.readByte();
			int noOfCells = table.readByte();
			
			int mid = (int) Math.ceil(noOfCells/2);
			
			int lower = mid-1;
			int upper = noOfCells - lower;
			int content = 512;

			for(int i = mid; i <= noOfCells; i++){
				
				table.seek(currentPage*PAGESIZE+8+(2*i)-2);
				short offset=table.readShort();
				table.seek(offset);
				
				int cellSize = table.readShort()+6;
				content = content - cellSize;
				
				table.seek(offset);
				byte[] cell = new byte[cellSize];
				table.read(cell);
			
				table.seek((newPage-1)*PAGESIZE+content);
				table.write(cell);
				
				table.seek((newPage-1)*PAGESIZE+8+(i-mid)*2);
				table.writeShort((newPage-1)*PAGESIZE+content);
				
			}

			// cell start area
			table.seek((newPage-1)*PAGESIZE+2);
			table.writeShort((newPage-1)*PAGESIZE+content);

			
			//current page cell content area update
			table.seek((currentPage)*PAGESIZE+8+(lower*2));
			short offset=table.readShort();
			table.seek((currentPage)*PAGESIZE+2);
			table.writeShort(offset);

			
			
			//copy right pointer of current page to new page
			table.seek((currentPage)*PAGESIZE+4);
			int rightpointer = table.readInt();
			table.seek((newPage-1)*PAGESIZE+4);
			table.writeInt(rightpointer);
			//update current page
			table.seek((currentPage)*PAGESIZE+4);
			table.writeInt(newPage); //CHECK HERE NP
			

			
			byte cells = (byte) lower;
			table.seek((currentPage)*PAGESIZE+1);
			table.writeByte(cells);
			cells = (byte) upper;
			table.seek((newPage-1)*PAGESIZE+1);
			table.writeByte(cells);
			
			//parent updation
			int parent = getParent(table,currentPage+1);
			if(parent==0){
				int parentpage = createInteriorPage(table);
				setParent(table,parentpage,currentPage,midKey);
				table.seek((parentpage-1)*PAGESIZE+4);
				table.writeInt(newPage); // right child
			}
			else
			{
				if(checkforRightPointer(table,parent,currentPage+1))
				{
					setParent(table,parent,currentPage,midKey);
					table.seek((parent-1)*PAGESIZE+4);
					table.writeInt(newPage); // right child
				}
				else{
					setParent(table,parent,newPage,midKey);
				}
			}
		}catch(Exception e){
			System.out.println("Error at Split_leaf_PAGE");
			e.printStackTrace();
		}
	}
	
	
	private static void setParent(RandomAccessFile table, int parent,  int childPage, int midkey) {
		// TODO Auto-generated method stub
		try {
			table.seek((parent-1)*PAGESIZE+1);
			int numrecords = table.read();
			if(checkInteriorRecordFit(table,parent))
			{
				
				int content=(parent)*PAGESIZE;
				TreeMap<Integer,Short> offsets = new TreeMap<Integer,Short>();
				if(numrecords==0){
					table.seek((parent-1)*PAGESIZE+1);
					table.write(1);
					content = content-8;
					table.writeShort(content);  //cell content star
					table.writeInt(-1);		//right page pointer
					table.writeShort(content);	//offset arrays
					table.seek(content);
					table.writeInt(childPage+1);
					table.writeInt(midkey);

				}
				else{
					table.seek((parent-1)*PAGESIZE+2);
					short cellContentArea = table.readShort();
					cellContentArea = (short) (cellContentArea-8);
					table.seek(cellContentArea);
					table.writeInt(childPage+1);
					table.writeInt(midkey);
					table.seek((parent-1)*PAGESIZE+2);
					table.writeShort(cellContentArea);
					for(int i=0;i<numrecords;i++){
						table.seek((parent-1)*PAGESIZE+8+2*i);
						short off = table.readShort();
						table.seek(off+4);
						int key = table.readInt();
						offsets.put(key, off);
					}
					offsets.put(midkey,cellContentArea);
					table.seek((parent-1)*PAGESIZE+1);
					table.write(numrecords++);
					table.seek((parent-1)*PAGESIZE+8);
					for(Entry<Integer, Short> entry : offsets.entrySet()) {
						table.writeShort(entry.getValue());
					}
				}
			}
			else{
				splitInteriorPage(table,parent);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	private static void splitInteriorPage(RandomAccessFile table, int parent) {
		
		
		int newPage = createInteriorPage(table);
		int midKey = divideData(table, parent-1);
		writeContentInteriorPage(table,parent,newPage,midKey);
		
		
		try {
			table.seek((parent-1)*PAGESIZE+4);
			int rightpage = table.readInt();
			table.seek((newPage-1)*PAGESIZE+4);
			table.writeInt(rightpage);
			table.seek((parent-1)*PAGESIZE+4);
			table.writeInt(newPage);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	
	private static void writeContentInteriorPage(RandomAccessFile table, int parent, int newPage, int midKey) {
		// TODO Auto-generated method stub
		try {
			table.seek((parent-1)*PAGESIZE+1);
			int numrecords = table.read();
			int mid = (int) Math.ceil((double)numrecords/2);
			int numrecords1 = mid-1;
			int numrecords2 = numrecords-numrecords1;
			int size = PAGESIZE;
			for(int i=numrecords1;i<numrecords;i++)
			{
				table.seek((parent-1)*PAGESIZE+8+2*i);
				short offset = table.readShort();
				table.seek(offset);
				byte[] data = new byte[8];
				table.read(data);
				size = size-8;
				table.seek((newPage-1)*PAGESIZE+size);
				table.write(data);
				
				//setting offset
				table.seek((newPage-1)*PAGESIZE+8+(i-numrecords1)*2);
				table.writeShort(size);
				
			}
			
			//setting number of records
			table.seek((parent-1)*PAGESIZE+1);
			table.write(numrecords1);
			
			table.seek((newPage-1)*PAGESIZE+1);
			table.write(numrecords2);
			
			int int_parent = getParent(table, parent);
			if(int_parent==0){
				int newParent = createInteriorPage(table);
				setParent(table, newParent, parent, midKey);
				table.seek((newParent-1)*PAGESIZE+4);
				table.writeInt(newPage);
			}
			else{
				if(checkforRightPointer(table,int_parent,parent)){
					setParent(table, int_parent, parent, midKey);
					table.seek((int_parent-1)*PAGESIZE+4);
					table.writeInt(newPage);
				}
				else
				setParent(table, int_parent, newPage, midKey);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static boolean checkforRightPointer(RandomAccessFile table, int parent, int rightPointer) {
		
		try {
			table.seek((parent-1)*PAGESIZE+4);
			if(table.readInt()==rightPointer)
				return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private static boolean checkInteriorRecordFit(RandomAccessFile table, int parent) {
		
		try {
			table.seek((parent - 1) * PAGESIZE + 1);
			int numrecords = table.read();
			short cellcontent = table.readShort();
			int size = 8 + numrecords * 2 + cellcontent;
			size = PAGESIZE - size;
			if (size >= 8)
				return true;
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		return false;
	}
	
	
	private static int createInteriorPage(RandomAccessFile table) {
		
		int numpages =0;
		try {
			numpages= (int) (table.length()/PAGESIZE);
			numpages++;
			table.setLength(table.length()+PAGESIZE);
			table.seek((numpages-1)*PAGESIZE);
			table.write(0x05);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return numpages;
	}
	
	

	private static int getParent(RandomAccessFile table, int page) {

		try {
			int numpages = (int) (table.length() / PAGESIZE);
			for (int i = 0; i < numpages; i++) {

				table.seek(i * PAGESIZE);
				byte pageType = table.readByte();

				if (pageType == 0x05) {
					table.seek(i * PAGESIZE + 4);
					int p = table.readInt();
					if (page == p)
						return i + 1;

					table.seek(i * PAGESIZE + 1);
					int numrecords = table.read();
					short[] offsets = new short[numrecords];

					// insertFile.read(offsets);
					for (int j = 0; j < numrecords; j++) {
						table.seek(i * PAGESIZE + 8 + 2 * j);
						offsets[i] = table.readShort();
						table.seek(offsets[i]);
						if (page == table.readInt())
							return j + 1;

					}

				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}
	
	
	

	private static int divideData(RandomAccessFile table,int pageNo)
	{
		int midKey=0;
		try{
			table.seek((pageNo)*PAGESIZE);
			byte pageType = table.readByte();
			short numCells = table.readByte();
			// id of mid cell
			short mid = (short) Math.ceil(numCells/2);
			
			table.seek(pageNo*PAGESIZE+8+(2*(mid-1)));
			short addr=table.readShort();
			table.seek(addr);
			
			if(pageType==0x0D)
				table.seek(addr+2);
			else 
				table.seek(addr+4);
		
			midKey=table.readInt();
			
			
		}catch(Exception e){
			e.printStackTrace();
		}

		return midKey;

	}
	
	

	private static int createNewPage(RandomAccessFile table) {
		
		try{
			int noOfPages = (int)table.length()/PAGESIZE;
			noOfPages = noOfPages + 1;
			table.setLength(noOfPages*PAGESIZE);
			table.seek((noOfPages-1)*PAGESIZE);
			table.writeByte(0x0D);
			return noOfPages;
		}catch(Exception e){
			e.printStackTrace();
		}

		return -1;
	}


	public static void writePayload(RandomAccessFile file, Cell cell,int cellLocation)
	{
		
		try{
			
		file.seek(cellLocation);
		file.writeShort(cell.Get_payload_Size());
		file.writeInt(cell.getRowId());
		
		Payload payload=cell.Get_Payload();
		file.writeByte(payload.getNumberOfColumns());
		
		byte[] dataTypes=payload.getDataType();
		file.write(dataTypes);
		
		String data[]=payload.getData();
		
		for(int i = 0; i < dataTypes.length; i++){
			switch(dataTypes[i]){
				case 0x00:
					file.writeByte(0);
					break;
				case 0x01:
					file.writeShort(0);
					break;
				case 0x02:
					file.writeInt(0);
					break;
				case 0x03:
					file.writeLong(0);
					break;
				case 0x04:
					file.writeByte(new Byte(data[i+1]));
					break;
				case 0x05:
					file.writeShort(new Short(data[i+1]));
					break;
				case 0x06:
					file.writeInt(new Integer(data[i+1]));
					break;
				case 0x07:
					file.writeLong(new Long(data[i+1]));
					break;
				case 0x08:
					file.writeFloat(new Float(data[i+1]));
					break;
				case 0x09:
					file.writeDouble(new Double(data[i + 1]));
					break;
				case 0x0A:
					long datetime = file.readLong();
					ZoneId zoneId = ZoneId.of("America/Chicago");
					Instant x = Instant.ofEpochSecond(datetime);
					ZonedDateTime zdt2 = ZonedDateTime.ofInstant(x, zoneId);
					zdt2.toLocalTime();
					// file.writeBytes(zdt2.toLocalDateTime().toString());
					break;
				case 0x0B:
					long date = file.readLong();
					ZoneId zoneId1 = ZoneId.of("America/Chicago");
					Instant x1 = Instant.ofEpochSecond(date);
					ZonedDateTime zdt3 = ZonedDateTime.ofInstant(x1, zoneId1);
					// file.writeBytes(zdt3.toLocalDate().toString());
					break;
				default:
					file.writeBytes(data[i + 1]);
					break;
			}
		}
		
		//update no of cells
		file.seek((PAGESIZE*cell.Get_page_number())+1);
		int noOfCells = file.readByte();
		
		file.seek((PAGESIZE*cell.Get_page_number())+1);
		file.writeByte((byte)(noOfCells+1));
		
		
		//update cell start offset
		
		//update cell arrays
		//getPositionMethod
		Map<Integer,Short> updateMap=new TreeMap<Integer,Short>();
		short[] cellLocations = new short[noOfCells];
		int[] keys=new int[noOfCells];
		
		for (int location = 0; location < noOfCells; location++) {
			
			file.seek((PAGESIZE * cell.Get_page_number())+8+(location*2));
			cellLocations[location] = file.readShort();
			file.seek(cellLocations[location]+2);
			keys[location]=file.readInt();
			updateMap.put(keys[location], cellLocations[location]);
		}
		updateMap.put(cell.getRowId(), (short)cellLocation);
		
		//update Cell Array in ascending order
		file.seek((PAGESIZE * cell.Get_page_number()) + 8);
		for (Map.Entry<Integer, Short> entry : updateMap.entrySet()) {
			short offset=entry.getValue();
			file.writeShort(offset);
		}
		
		
		//update cell start area
		file.seek((PAGESIZE * cell.Get_page_number())+2);
		file.writeShort(cellLocation);
		file.close();
		
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}	
	}
	
	public static Cell createCell(int pageNo,int primaryKey,short payLoadSize,byte[] dataType,String[] values)
	{
		Cell cell=new Cell();
		cell.Set_page_number(pageNo);
		cell.setRowId(primaryKey);
		cell.Set_Payload_Size(payLoadSize);
		
		
		Payload payload=new Payload();
		payload.setNumberOfColumns(Byte.parseByte(values.length-1+""));
		payload.setDataType(dataType);
		payload.setData(values);	
		
		cell.set_Payload(payload);
		
		return cell;
	}
	

	public static int checkPageOverFlow(RandomAccessFile file, int page, int payLoadsize){
		int val = -1;

		try{
			file.seek((page)*PAGESIZE+2);
			int content = file.readShort();
			if(content == 0)
				return PAGESIZE - payLoadsize;
			/*
			file.seek((page)*PAGESIZE+1);
			
			int numCells = file.readByte();
			int space = content - (8 + 2*numCells + 2);
			if(payLoadsize <= space)
				return content - payLoadsize;*/
			
			
			file.seek((page)*PAGESIZE+1);
			int noOfCells=file.read();
			int pageHeaderSize=8+2*noOfCells+2;
			
			file.seek((page)*PAGESIZE+2);
			short startArea =(short)((page+1)*PAGESIZE- file.readShort());
			
			int space=startArea+pageHeaderSize;
			int spaceAvail = PAGESIZE-space;
			
			if(spaceAvail>=payLoadsize){
				file.seek((page)*PAGESIZE+2);
				short offset=file.readShort();
				return offset-payLoadsize;
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}

		return val;
	}

	private static int Get_payload_Size(String tableName, String[] values, byte[] plDataType, String[] dataType) {

		int size = 1 + dataType.length - 1;
		for (int i = 0; i < values.length-1; i++) {
			plDataType[i] = getDataTypeCode(values[i + 1], dataType[i + 1]);
			size=size+dataLength(plDataType[i]);
		}

		return size;
	}

	private static byte getDataTypeCode(String value, String dataType) {
		if (value.equals("null")) {
			switch (dataType) {
			case "TINYINT":
				return 0x00;
			case "SMALLINT":
				return 0x01;
			case "INT":
				return 0x02;
			case "BIGINT":
				return 0x03;
			case "REAL":
				return 0x02;
			case "DOUBLE":
				return 0x03;
			case "DATETIME":
				return 0x03;
			case "DATE":
				return 0x03;
			case "TEXT":
				return 0x03;
			default:
				return 0x00;
			}
		} else {
			switch (dataType) {
			case "TINYINT":
				return 0x04;
			case "SMALLINT":
				return 0x05;
			case "INT":
				return 0x06;
			case "BIGINT":
				return 0x07;
			case "REAL":
				return 0x08;
			case "DOUBLE":
				return 0x09;
			case "DATETIME":
				return 0x0A;
			case "DATE":
				return 0x0B;
			case "TEXT":
				return (byte) (value.length() + 0x0C);
			default:
				return 0x00;
			}
		}
	}

	private static short dataLength(byte codes) {
		switch (codes) {
		case 0x00:
			return 1;
		case 0x01:
			return 2;
		case 0x02:
			return 4;
		case 0x03:
			return 8;
		case 0x04:
			return 1;
		case 0x05:
			return 2;
		case 0x06:
			return 4;
		case 0x07:
			return 8;
		case 0x08:
			return 4;
		case 0x09:
			return 8;
		case 0x0A:
			return 8;
		case 0x0B:
			return 8;
		default:
			return (short) (codes - 0x0C);
		}
	}
	
	
	public static int getPageNo(String tableName, int key) {
		try {

			tableName=tableName.trim();
			String path="data/userdata/"+tableName+".tbl";
			if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path="data/catalog/"+tableName+".tbl";
			
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);
			
			Map<Integer, String> colNames = getColumnNames(tableName);
			Map<Integer, Cell> records = new LinkedHashMap<Integer, Cell>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {
					
					int noOfCells = table.readByte();
					short[] cellLocations = new short[noOfCells];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = table.readShort();
					}
					Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
					recordCells = getRecords(table, cellLocations,i);
					
					Set<Integer> rowIds=recordCells.keySet();
					
					Set<Integer> sortedRowIds = new TreeSet<Integer>(rowIds);
					
					Integer rows[]=sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
					
					//last page
					table.seek((PAGESIZE * i)+4);
					int filePointer = table.readInt();
					
					if(rowIds.size()==0)
						return 0;
					if(rows[0] <= key && key <= rows[rows.length - 1])
						return i;
					else if(filePointer== -1 && rows[rows.length-1]<key)
					    return i;
				}
			}

		}
			
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return -1;
	}
	


	public static Map<Integer, Cell> getData(String tableName, String[] columnNames, String[] condition) {
		try {

			
			tableName=tableName.trim();
			String path="data/userdata/"+tableName+".tbl";
			if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path="data/catalog/"+tableName+".tbl";
			
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);
			
			Map<Integer,Page> pageInfo=new LinkedHashMap<Integer, Page>();
			
			
			Map<Integer, String> colNames = getColumnNames(tableName);
			Map<Integer, Cell> records = new LinkedHashMap<Integer, Cell>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {
					
					Page page=new Page();
					page.Set_page_number(i);
					page.Set_Page_Type(pageType);
					
					
					
					int noOfCells = table.readByte();
					short[] cellLocations = new short[noOfCells];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = table.readShort();
					}
					Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
					recordCells = getRecords(table, cellLocations,i);
					
					page.Set_Records(recordCells);
					pageInfo.put(i, page);
					
					records.putAll(recordCells);
					//System.out.println(recordCells);
				}
			}

			if (condition.length > 0) {
				Map<Integer, Cell> filteredRecords = filterRecords(colNames, records, columnNames, condition);
				// printTable(colNames, filteredRecords);
				return filteredRecords;
			} else {
				//printTable(colNames, records);
				return records;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;

	}

	public static String[] getDataType(Map<Integer, Cell> columnsMeta) {
		int count = 0;
		String[] dataType = new String[columnsMeta.size()];
		for (Map.Entry<Integer, Cell> entry : columnsMeta.entrySet()) {

			Cell cell = entry.getValue();
			Payload payload = cell.Get_Payload();
			String[] data = payload.getData();
			dataType[count] = data[2];
			count++;
		}
		return dataType;
	}

	public static String[] isNullable(Map<Integer, Cell> columnsMeta) {
		int count = 0;
		String[] nullable = new String[columnsMeta.size()];
		for (Map.Entry<Integer, Cell> entry : columnsMeta.entrySet()) {

			Cell cell = entry.getValue();
			Payload payload = cell.Get_Payload();
			String[] data = payload.getData();
			nullable[count] = data[4];
			count++;
		}
		return nullable;
	}

	public static Map<Integer, Cell> getColumnsMeta(String tableName, String[] columnNames, String[] condition) {

		try {
			
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);

			Map<Integer, String> colNames = getColumnNames("davisbase_columns");
			Map<Integer, Cell> records = new LinkedHashMap<Integer, Cell>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {

					int noOfCells = table.readByte();
					short[] cellLocations = new short[noOfCells];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = table.readShort();
					}
					Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
					recordCells = getRecords(table, cellLocations,i);
					records.putAll(recordCells);
					//System.out.println(recordCells);
				}
			}

			if (condition.length > 0) {
				Map<Integer, Cell> filteredRecords = filterRecords(colNames, records, columnNames, condition);
				// printTable(colNames, filteredRecords);
				return filteredRecords;
			} else {
				return records;
				// printTable(colNames, records);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;

	}

	public static void printTable(Map<Integer, String> colNames, Map<Integer, Cell> records) {
		String colString = "";
		String recString = "";
		ArrayList<String> recList = new ArrayList<String>();

		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {

			String colName = entry.getValue();
			colString += colName + " | ";
		}
		System.out.println(colString);
		for (Map.Entry<Integer, Cell> entry : records.entrySet()) {

			Cell cell = entry.getValue();
			recString += cell.getRowId();
			String data[] = cell.Get_Payload().getData();
			for (String dataS : data) {
				recString = recString + " | " + dataS;
			}
			System.out.println(recString);
			recString = "";
		}

	}

	public static void Query(String tableName, String[] columnNames, String[] condition) {

		try {

			tableName=tableName.trim();
			String path="data/userdata/"+tableName+".tbl";
			if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path="data/catalog/"+tableName+".tbl";
			
			
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);

			Map<Integer, String> colNames = getColumnNames(tableName);
			Map<Integer, Cell> records = new LinkedHashMap<Integer, Cell>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {

					int noOfCells = table.readByte();
					short[] cellLocations = new short[noOfCells];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = table.readShort();
					}
					Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
					recordCells = getRecords(table, cellLocations,i);
					records.putAll(recordCells);
					//System.out.println(recordCells);
				}
			}

			if (condition.length > 0) {
				Map<Integer, Cell> filteredRecords = filterRecords(colNames, records, columnNames, condition);
				printTable(colNames, filteredRecords);
			} else {
				printTable(colNames, records);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static Map<Integer, Cell> filterRecords(Map<Integer, String> colNames, Map<Integer, Cell> records,
			String[] resultColumnNames, String[] condition) {

		Set<String> resultColumnSet = new HashSet<String>(Arrays.asList(resultColumnNames));
		Map<Integer, Cell> filteredRecords = new LinkedHashMap<Integer, Cell>();
		/*
		 * for (Map.Entry<Integer, String> entry : colNames.entrySet()) { String
		 * columnName=entry.getValue(); if(resultColumnSet.contains(columnName))
		 * colNames.remove(entry.getKey());
		 * //ordinalPosition.add(entry.getKey()); }
		 */
		int whereOrdinalPosition = 2;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				whereOrdinalPosition = entry.getKey();
			}
		}
		Set<Integer> ordinalPositions = colNames.keySet();
		for (Map.Entry<Integer, Cell> entry : records.entrySet()) {
			Cell cell = entry.getValue();
			Payload payload = cell.Get_Payload();
			String[] data = payload.getData();
			byte[] dataTypeCodes = payload.getDataType();

			boolean result;
			if (whereOrdinalPosition == 1)
				result = checkData((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = checkData(dataTypeCodes[whereOrdinalPosition - 2], data[whereOrdinalPosition - 2], condition);

			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		}

		return filteredRecords;

	}
	
	
	private static Map<Integer, Cell> filterRecords2(Map<Integer, String> colNames, Map<Integer, Cell> records,
			String[] resultColumnNames, String[] condition) {

		Set<String> resultColumnSet = new HashSet<String>(Arrays.asList(resultColumnNames));
		Map<Integer, Cell> filteredRecords = new LinkedHashMap<Integer, Cell>();
		/*
		 * for (Map.Entry<Integer, String> entry : colNames.entrySet()) { String
		 * columnName=entry.getValue(); if(resultColumnSet.contains(columnName))
		 * colNames.remove(entry.getKey());
		 * //ordinalPosition.add(entry.getKey()); }
		 */
		int whereOrdinalPosition = -1;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				whereOrdinalPosition = entry.getKey();
			}
		}
		
		Set<Integer> ordinalPositions = colNames.keySet();
		for (Map.Entry<Integer, Cell> entry : records.entrySet()) {
			Cell cell = entry.getValue();
			Payload payload = cell.Get_Payload();
			String[] data = payload.getData();
			byte[] dataTypeCodes = payload.getDataType();

			boolean result;
			if (whereOrdinalPosition == 1)
				result = checkData((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = checkData(dataTypeCodes[whereOrdinalPosition - 2], data[whereOrdinalPosition - 2], condition);

			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		}

		return filteredRecords;

	}
	
	private static Map<Integer, Cell> filterRecordsByData(Map<Integer, String> colNames, Map<Integer, Cell> records,
			String[] resultColumnNames, String[] condition) {

		Set<String> resultColumnSet = new HashSet<String>(Arrays.asList(resultColumnNames));
		Map<Integer, Cell> filteredRecords = new LinkedHashMap<Integer, Cell>();
		
		int whereOrdinalPosition = 2;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				whereOrdinalPosition = entry.getKey();
			}
		}
		Set<Integer> ordinalPositions = colNames.keySet();
		for (Map.Entry<Integer, Cell> entry : records.entrySet()) {
			Cell cell = entry.getValue();
			Payload payload = cell.Get_Payload();
			String[] data = payload.getData();
			byte[] dataTypeCodes = payload.getDataType();

			boolean result;
			if (whereOrdinalPosition == 1)
				result = checkData((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = checkData(dataTypeCodes[whereOrdinalPosition - 2], data[whereOrdinalPosition - 2], condition);

			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		}

		return filteredRecords;

	}
	
	
	
	private static boolean checkData(byte code, String data, String[] condition) {

		if (code >= 0x04 && code <= 0x07) {
			Long dataLong = Long.parseLong(data);
			switch (condition[1]) {
			case "=":
				if (dataLong == Long.parseLong(condition[2]))
					return true;
				break;
			case ">":
				if (dataLong > Long.parseLong(condition[2]))
					return true;
				break;
			case "<":
				if (dataLong < Long.parseLong(condition[2]))
					return true;
				break;
			case "<=":
				if (dataLong <= Long.parseLong(condition[2]))
					return true;
				break;
			case ">=":
				if (dataLong >= Long.parseLong(condition[2]))
					return true;
				break;
			case "<>":
				if (dataLong != Long.parseLong(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}

		} else if (code == 0x08 || code == 0x09) {
			Double doubleData = Double.parseDouble(data);
			switch (condition[1]) {
			case "=":
				if (doubleData == Double.parseDouble(condition[2]))
					return true;
				break;
			case ">":
				if (doubleData > Double.parseDouble(condition[2]))
					return true;
				break;
			case "<":
				if (doubleData < Double.parseDouble(condition[2]))
					return true;
				break;
			case "<=":
				if (doubleData <= Double.parseDouble(condition[2]))
					return true;
				break;
			case ">=":
				if (doubleData >= Double.parseDouble(condition[2]))
					return true;
				break;
			case "<>":
				if (doubleData != Double.parseDouble(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}

		} else if (code >= 0x0C) {

			condition[2] = condition[2].replaceAll("'", "");
			condition[2] = condition[2].replaceAll("\"", "");
			switch (condition[1]) {
			case "=":
				if (data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			case "<>":
				if (!data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}
		}

		return false;

	}

	public static Map<Integer, String> getColumnNames(String tableName) {
		Map<Integer, String> columns = new LinkedHashMap<Integer, String>();
		try {
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);

			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {

					int noOfCells = table.readByte();
					short[] cellLocations = new short[noOfCells];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = table.readShort();
					}
					Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
					recordCells = getRecords(table, cellLocations,i);

					for (Map.Entry<Integer, Cell> entry : recordCells.entrySet()) {

						Cell cell = entry.getValue();
						// System.out.println("Key : " + entry.getKey() + "
						// Value : " + entry.getValue());
						Payload payload = cell.Get_Payload();
						String[] data = payload.getData();
						if (data[0].equalsIgnoreCase(tableName)) {
							columns.put(Integer.parseInt(data[3]), data[1]);
						}

					}

					//System.out.println(recordCells);
				}

			}
		} catch (Exception e) {

			e.printStackTrace();
		}

		return columns;

	}

	private static Map<Integer, Cell> getRecords(RandomAccessFile table, short[] cellLocations, int pageNo) {

		Map<Integer, Cell> cells = new LinkedHashMap<Integer, Cell>();
		for (int position = 0; position < cellLocations.length; position++) {
			try {
				Cell cell = new Cell();
				cell.Set_page_number(pageNo);
				cell.setCellLocation(cellLocations[position]);
				
				table.seek(cellLocations[position]);

				short payLoadSize = table.readShort();
				cell.Set_Payload_Size(payLoadSize);

				int rowId = table.readInt();
				cell.setRowId(rowId);

				Payload payload = new Payload();
				byte num_cols = table.readByte();
				payload.setNumberOfColumns(num_cols);

				byte[] dataType = new byte[num_cols];
				int colsRead = table.read(dataType);
				payload.setDataType(dataType);

				String data[] = new String[num_cols];
				payload.setData(data);

				for (int i = 0; i < num_cols; i++) {
					switch (dataType[i]) {
					case 0x00:
						data[i] = Integer.toString(table.readByte());
						data[i] = "null";
						break;

					case 0x01:
						data[i] = Integer.toString(table.readShort());
						data[i] = "null";
						break;

					case 0x02:
						data[i] = Integer.toString(table.readInt());
						data[i] = "null";
						break;

					case 0x03:
						data[i] = Long.toString(table.readLong());
						data[i] = "null";
						break;

					case 0x04:
						data[i] = Integer.toString(table.readByte());
						break;

					case 0x05:
						data[i] = Integer.toString(table.readShort());
						break;

					case 0x06:
						data[i] = Integer.toString(table.readInt());
						break;

					case 0x07:
						data[i] = Long.toString(table.readLong());
						break;

					case 0x08:
						data[i] = String.valueOf(table.readFloat());
						break;

					case 0x09:
						data[i] = String.valueOf(table.readDouble());
						break;

					case 0x0A:
						long tmp = table.readLong();
						Date dateTime = new Date(tmp);
						// data[i] = formater.format(dateTime);
						break;

					case 0x0B:
						long tmp1 = table.readLong();
						Date date = new Date(tmp1);
						// data[i] = formater.format(date).substring(0,10);
						break;

					default:
						int len = new Integer(dataType[i] - 0x0C);
						byte[] bytes = new byte[len];
						for (int j = 0; j < len; j++)
							bytes[j] = table.readByte();
						data[i] = new String(bytes);
						break;
					}

				}

				cell.set_Payload(payload);
				cells.put(rowId, cell);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return cells;
	}

	public static void initializeDatabase() {

		File data = new File("data/catalog");
		File userData=new File("data/userdata");
		data.mkdir();
		userData.mkdir();
		RandomAccessFile tablesMeta;
		RandomAccessFile davisbaseColumnsCatalog;

		createDavisBase_Tables();
		createDavisBase_Columns();

	}

	public static void createDavisBase_Tables() {

		try {
			RandomAccessFile tablesMeta = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			tablesMeta.setLength(PAGESIZE * 1);
			tablesMeta.seek(0);
			tablesMeta.write(0x0D);
			tablesMeta.write(0x02);
			tablesMeta.writeShort(PAGESIZE - 32 - 33);
			tablesMeta.writeInt(-1);// rightmost
			tablesMeta.writeShort(PAGESIZE - 32);
			tablesMeta.writeShort(PAGESIZE - 32 - 33);

			tablesMeta.seek(PAGESIZE - 32);
			tablesMeta.writeShort(26);
			tablesMeta.writeInt(1);
			tablesMeta.writeByte(3);
			tablesMeta.writeByte(28);
			tablesMeta.write(0x06);
			tablesMeta.write(0x05);
			tablesMeta.writeBytes("davisbase_tables");
			tablesMeta.writeInt(2);
			tablesMeta.writeShort(34); // avg_length

			tablesMeta.seek(PAGESIZE - 32 - 33);
			tablesMeta.writeShort(19);
			tablesMeta.writeInt(2);
			tablesMeta.writeByte(3);
			tablesMeta.writeByte(29);
			tablesMeta.write(0x06);
			tablesMeta.write(0x05);
			tablesMeta.writeBytes("davisbase_columns");
			tablesMeta.writeInt(10);
			tablesMeta.writeShort(34); // avg_lngth

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void createDavisBase_Columns() {

		int cellHeader = 6;
		try {

			RandomAccessFile columnsMeta = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			columnsMeta.setLength(PAGESIZE * 1);
			columnsMeta.seek(0);
			columnsMeta.write(0x0D);
			columnsMeta.write(10);

			int recordSize[] = new int[] { 33, 39, 40, 43, 34, 40, 41, 39, 49, 41 };
			int offset[] = new int[10];

			offset[0] = PAGESIZE - recordSize[0] - cellHeader;

			// error
			columnsMeta.seek(4);

			columnsMeta.writeInt(-1);

			//columnsMeta.writeShort(offset[0]);
			for (int i = 1; i < offset.length; i++) {
				offset[i] = offset[i - 1] - (recordSize[i] + cellHeader);

			}
			columnsMeta.seek(2);
			columnsMeta.writeShort(offset[9]);

			columnsMeta.seek(8);
			for (int i = 0; i < offset.length; i++) {
				columnsMeta.writeShort(offset[i]);
			}

			// 1
			columnsMeta.seek(offset[0]);
			columnsMeta.writeShort(recordSize[0]);
			columnsMeta.writeInt(1);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(17);
			columnsMeta.write(15);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_tables");
			columnsMeta.writeBytes("rowid");
			columnsMeta.writeBytes("INT");
			columnsMeta.write(1);
			columnsMeta.writeBytes("NO");

			// 2
			columnsMeta.seek(offset[1]);
			columnsMeta.writeShort(recordSize[1]);
			columnsMeta.writeInt(2);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(22);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_tables");
			columnsMeta.writeBytes("table_name");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(2);
			columnsMeta.writeBytes("NO");

			// 3
			columnsMeta.seek(offset[2]);
			columnsMeta.writeShort(recordSize[2]);
			columnsMeta.writeInt(3);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(24);
			columnsMeta.write(15);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_tables");
			columnsMeta.writeBytes("record_count");
			columnsMeta.writeBytes("INT");
			columnsMeta.write(3);
			columnsMeta.writeBytes("NO");

			// 4
			columnsMeta.seek(offset[3]);
			columnsMeta.writeShort(recordSize[3]);
			columnsMeta.writeInt(4);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(22);
			columnsMeta.write(20);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_tables");
			columnsMeta.writeBytes("avg_length");
			columnsMeta.writeBytes("SMALLINT");
			columnsMeta.write(4);
			columnsMeta.writeBytes("NO");

			// 5
			columnsMeta.seek(offset[4]);
			columnsMeta.writeShort(recordSize[4]);
			columnsMeta.writeInt(5);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(17);
			columnsMeta.write(15);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("rowid");
			columnsMeta.writeBytes("INT");
			columnsMeta.write(1);
			columnsMeta.writeBytes("NO");

			// 6
			columnsMeta.seek(offset[5]);
			columnsMeta.writeShort(recordSize[5]);
			columnsMeta.writeInt(6);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(22);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("table_name");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(2);
			columnsMeta.writeBytes("NO");

			// 7
			columnsMeta.seek(offset[6]);
			columnsMeta.writeShort(recordSize[6]);
			columnsMeta.writeInt(7);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(23);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("column_name");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(3);
			columnsMeta.writeBytes("NO");

			// 8
			columnsMeta.seek(offset[7]);
			columnsMeta.writeShort(recordSize[7]);
			columnsMeta.writeInt(8);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(21);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("data_type");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(4);
			columnsMeta.writeBytes("NO");

			// 9
			columnsMeta.seek(offset[8]);
			columnsMeta.writeShort(recordSize[8]);
			columnsMeta.writeInt(9);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(28);
			columnsMeta.write(19);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("ordinal_position");
			columnsMeta.writeBytes("TINYINT");
			columnsMeta.write(5);
			columnsMeta.writeBytes("NO");

			// 10
			columnsMeta.seek(offset[9]);
			columnsMeta.writeShort(recordSize[9]);
			columnsMeta.writeInt(10);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(23);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("davisbase_columns");
			columnsMeta.writeBytes("is_nullable");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(6);
			columnsMeta.writeBytes("NO");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static String[] parseCondition(String whereCondition) {

		String condition[] = new String[3];
		String values[] = new String[2];
		if (whereCondition.contains("=")) {
			values = whereCondition.split("=");
			condition[0] = values[0].trim();
			condition[1] = "=";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains(">")) {
			values = whereCondition.split(">");
			condition[0] = values[0].trim();
			condition[1] = ">";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains("<")) {
			values = whereCondition.split("<");
			condition[0] = values[0].trim();
			condition[1] = "<";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains(">=")) {
			values = whereCondition.split(">=");
			condition[0] = values[0].trim();
			condition[1] = ">=";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains("<=")) {
			values = whereCondition.split("<=");
			condition[0] = values[0].trim();
			condition[1] = "<=";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains("<>")) {
			values = whereCondition.split("<>");
			condition[0] = values[0].trim();
			condition[1] = "<>";
			condition[2] = values[1].trim();
		}

		return condition;
	}


	public static void createTable(RandomAccessFile table,String tableName, String[] columnNames) {
		
		try{
		
			
			
			// configure new blank page
			table.setLength(PAGESIZE);
			table.seek(0);
			table.writeByte(0x0D);
			table.seek(2);
			table.writeShort(PAGESIZE);
			table.writeInt(-1);
			table.close();
			
			//update Davisbase_tables
			RandomAccessFile davisbaseTables=new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			int noOfPages=(int) (davisbaseTables.length()/PAGESIZE);
			int page=0;
			
			Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
			for(int i=0;i<noOfPages;i++)
			{
				davisbaseTables.seek((i*PAGESIZE)+4);
				int filePointer=davisbaseTables.readInt();
				if(filePointer==-1){
					page=i;
					davisbaseTables.seek(i*PAGESIZE+1);
					int noOfCells = davisbaseTables.readByte();
					short[] cellLocations = new short[noOfCells];
					davisbaseTables.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = davisbaseTables.readShort();
					}
					recordCells = getRecords(davisbaseTables, cellLocations,i);
				}
			}
			davisbaseTables.close();
			Set<Integer> rowIds=recordCells.keySet();
			Set<Integer> sortedRowIds = new TreeSet<Integer>(rowIds);
			Integer rows[]=sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
			int key=rows[rows.length-1]+1;
			
			String[] values = {String.valueOf(key),tableName.trim(),"8","10"};
			Insert("davisbase_tables", values);
			
			//Update Davisbase_columns
			RandomAccessFile davisbaseColumns=new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			noOfPages=(int) (davisbaseColumns.length()/PAGESIZE);
			page=0;
			
			recordCells = new LinkedHashMap<Integer, Cell>();
			for(int i=0;i<noOfPages;i++)
			{
				davisbaseColumns.seek((i*PAGESIZE)+4);
				int filePointer=davisbaseColumns.readInt();
				if(filePointer==-1){
					page=i;
					davisbaseColumns.seek(i*PAGESIZE+1);
					int noOfCells = davisbaseColumns.readByte();
					short[] cellLocations = new short[noOfCells];
					davisbaseColumns.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = davisbaseColumns.readShort();
					}
					recordCells = getRecords(davisbaseColumns, cellLocations,i);
				}
			}
			rowIds=recordCells.keySet();
			sortedRowIds = new TreeSet<Integer>(rowIds);
			rows=sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
			key=rows[rows.length-1];
			
			for(int i = 0; i < columnNames.length; i++){
				key = key + 1;
				
				String[] coltemp = columnNames[i].split(" ");
				String isNullable="YES";
				
				if(coltemp.length==4)
				{
					if(coltemp[2].equalsIgnoreCase("NOT") && coltemp[3].equalsIgnoreCase("NULL"))
					{
						isNullable="NO";	
					}
					if(coltemp[2].equalsIgnoreCase("PRIMARY") && coltemp[3].equalsIgnoreCase("KEY"))
					{
						isNullable="NO";
					}
					
				}
				String colName = coltemp[0];
				String dataType = coltemp[1].toUpperCase();
				String ordinalPosition = String.valueOf(i+1);
				String[] val = {String.valueOf(key), tableName, colName, dataType, ordinalPosition, isNullable};
				Insert("davisbase_columns", val);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
	}


	public static void Drop_Table(String tableName) {
		
		try
		{
			RandomAccessFile davisbaseTables=new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			updateMetaOffset(davisbaseTables,"davisbase_tables",tableName);
			
			
			RandomAccessFile davisbaseColumns=new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			updateMetaOffset(davisbaseColumns,"davisbase_columns",tableName);
			
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
	}
	
	public static void updateMetaOffset(RandomAccessFile davisbaseTables,String metaTable,String tableName) throws IOException
	{
		int noOfPages = (int) (davisbaseTables.length() / PAGESIZE);

		Map<Integer, String> colNames = getColumnNames(metaTable);
		
		for (int i = 0; i < noOfPages; i++) {
			davisbaseTables.seek(PAGESIZE * i);
			byte pageType = davisbaseTables.readByte();
			if (pageType == 0x0D) {

				int noOfCells = davisbaseTables.readByte();
				short[] cellLocations = new short[noOfCells];
				davisbaseTables.seek((PAGESIZE * i) + 8);
				for (int location = 0; location < noOfCells; location++) {
					cellLocations[location] = davisbaseTables.readShort();
				}
				Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
				recordCells = getRecords(davisbaseTables, cellLocations,i);
				
				
				String[] condition={"table_name","<>",tableName};
				String[] columnNames={"*"};
				
				Map<Integer,Cell> filteredRecs=filterRecordsByData(colNames, recordCells, columnNames, condition);
				short[] offsets=new short[filteredRecs.size()];
				int l=0;
				for (Map.Entry<Integer, Cell> entry : filteredRecs.entrySet()){
					Cell cell=entry.getValue();
					offsets[l]=cell.getCellLocation();
					davisbaseTables.seek(i*PAGESIZE+8+(2*l));
					davisbaseTables.writeShort(offsets[l]);
					l++;
				}
				
				davisbaseTables.seek((PAGESIZE * i)+1);
				davisbaseTables.writeByte(offsets.length);
				davisbaseTables.writeShort(offsets[offsets.length-1]);
				//davisbaseTables.close();
			}
		}
	
	}

	public static void delete(String tableName, String[] cond) throws IOException {
		
		
		String path="data/userdata/"+tableName+".tbl";
		if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
			path="data/catalog/"+tableName+".tbl";
		
		
		try {
			RandomAccessFile table=new RandomAccessFile(path,"rw");
			
			String condition[] = { "table_name", "=", tableName };
			String columnNames[] = { "*" };
			Map<Integer, Cell> columnsMeta = getColumnsMeta(tableName, columnNames, condition);
			String[] dataType = getDataType(columnsMeta);
			String[] isNullable = isNullable(columnsMeta);
			Map<Integer, String> colNames = getColumnNames(tableName);
			
			condition = new String[0];
			
			//get page number on which data exist
			int pageNo=getPageNo(tableName,Integer.parseInt(cond[2]));
			
			//check for duplicate value
			Map<Integer, Cell> data = getData(tableName, columnNames, condition);
			if (data.containsKey(Integer.parseInt(cond[2]))) {
				table.seek((PAGESIZE * pageNo) + 1);
				int noOfCells = table.readByte();
				short[] cellLocations = new short[noOfCells];
				table.seek((PAGESIZE * pageNo) + 8);
				for (int location = 0; location < noOfCells; location++) {
					cellLocations[location] = table.readShort();
				}
				Map<Integer, Cell> recordCells = new LinkedHashMap<Integer, Cell>();
				recordCells = getRecords(table, cellLocations,pageNo);
				
				String[] condition1={cond[0],"<>",cond[2]};
				String[] columnNames1={"*"};
				
				Map<Integer,Cell> filteredRecs=filterRecordsByData(colNames, recordCells, columnNames, condition1);
				short[] offsets=new short[filteredRecs.size()];
				int l=0;
				for (Map.Entry<Integer, Cell> entry : filteredRecs.entrySet()){
					Cell cell=entry.getValue();
					offsets[l]=cell.getCellLocation();
					table.seek(pageNo*PAGESIZE+8+(2*l));
					table.writeShort(offsets[l]);
					l++;
				}
				
				table.seek((PAGESIZE*pageNo)+1);
				table.writeByte(offsets.length);
				table.writeShort(offsets[offsets.length-1]);
				table.close();
				
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
public static void Update(String tableName, String[] set, String[] cond) {
		
	String path="data/userdata/"+tableName+".tbl";
	if(tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
		path="data/catalog/"+tableName+".tbl";
	
	
	try {
		RandomAccessFile file=new RandomAccessFile(path,"rw");
		
		String condition[] = { "table_name", "=", tableName};
		String columnNames[] = { "*" };
		Map<Integer, Cell> columnsMeta = getColumnsMeta(tableName, columnNames, condition);
		String[] dataType = getDataType(columnsMeta);
		String[] isNullable = isNullable(columnsMeta);
		Map<Integer, String> colNames = getColumnNames(tableName);
		
		//ordinal position
		int k = -1;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(set[0])) {
				k = entry.getKey();
			}
		}
		
		if(cond.length>0){
		int key=Integer.parseInt(cond[2]);
		condition = new String[0];
		
		//get page number on which data exist
		int pageno=getPageNo(tableName,Integer.parseInt(cond[2]));
		
		//check for duplicate value
		Map<Integer, Cell> data = getData(tableName, columnNames, condition);
		if (data.containsKey(Integer.parseInt(cond[2]))) {
				
				try {
					file.seek((pageno)*PAGESIZE+1);
					int records = file.read();
					short[] offsetLocations = new short[records];
					//TreeMap<Integer, Short> offsets = new TreeMap<Integer, Short>();
					
					
					for(int j=0;j<records;j++){
						file.seek((pageno)*PAGESIZE+8+2*j);
						offsetLocations[j]=file.readShort();
						file.seek(offsetLocations[j]+2);
						//int pay_size = file.readShort();
						int ky=file.readInt();
							if(key==ky){
								int no=file.read();
								byte[] sc = new byte[no];
								file.read(sc);
								int seek_positions=0;
								for(int i=0;i<k-2;i++){
									seek_positions+=dataLength(sc[i]);
								}
								file.seek(offsetLocations[j]+6+no+1+seek_positions);
								
								
								byte sc_update = sc[k-2];
								switch (sc_update){
								
								case 0x00:	file.write(Integer.parseInt(set[2]));
											sc[k-2]=0x04;
											break;
								case 0x01:	file.writeShort(Integer.parseInt(set[2]));
											sc[k-2]=0x05;
											break;
								case 0x02:	file.writeInt(Integer.parseInt(set[2]));
											sc[k-2]=0x06;
											break;
								case 0x03:	file.writeDouble(Double.parseDouble(set[2]));
											sc[k-2]=0x09;
											break;
								case 0x04:	file.write(Integer.parseInt(set[2]));
											break;
								case 0x05:	file.writeShort(Integer.parseInt(set[2]));
											break;
								case 0x06:	file.writeInt(Integer.parseInt(set[2]));
											break;
								case 0x07:	file.writeLong(Long.parseLong(set[2]));
											break;
								
								case 0x08: 	file.writeFloat(Float.parseFloat(set[2]));
											break;
											
								case 0x09:	file.writeDouble(Double.parseDouble(set[2]));
											break;
											
								}
								
								file.seek(offsetLocations[j]+7);
								file.write(sc);

							}
					}
				
				}catch (Exception e) {
					e.printStackTrace(System.out);
				}
			}
		}
			else{
				
					try {
						int no_of_pages = (int) (file.length()/PAGESIZE);
						for(int l=0;l<no_of_pages;l++){
						file.seek(l*PAGESIZE);
						byte pageType=file.readByte();
						if(pageType==0x0D){
						
						file.seek((l)*PAGESIZE+1);
						int records = file.read();
						short[] offsetLocations = new short[records];
						
						for(int j=0;j<records;j++){
							file.seek((l)*PAGESIZE+8+2*j);
							offsetLocations[j]=file.readShort();
							file.seek(offsetLocations[j]+6);
							//int pay_size = file.readShort();
								
								
									int no=file.read();
									byte[] sc = new byte[no];
									file.read(sc);
									int seek_positions=0;
									for(int i=0;i<k-2;i++){
										seek_positions+=dataLength(sc[i]);
									}
									file.seek(offsetLocations[j]+6+no+1+seek_positions);
									
									
									byte sc_update = sc[k-2];
									switch (sc_update){
									
									case 0x00:	file.write(Integer.parseInt(set[2]));
												sc[k-2]=0x04;
												break;
									case 0x01:	file.writeShort(Integer.parseInt(set[2]));
												sc[k-2]=0x05;
												break;
									case 0x02:	file.writeInt(Integer.parseInt(set[2]));
												sc[k-2]=0x06;
												break;
									case 0x03:	file.writeDouble(Double.parseDouble(set[2]));
												sc[k-2]=0x09;
												break;
									case 0x04:	file.write(Integer.parseInt(set[2]));
												break;
									case 0x05:	file.writeShort(Integer.parseInt(set[2]));
												break;
									case 0x06:	file.writeInt(Integer.parseInt(set[2]));
												break;
									case 0x07:	file.writeLong(Long.parseLong(set[2]));
												break;
									
									case 0x08: 	file.writeFloat(Float.parseFloat(set[2]));
												break;
												
									case 0x09:	file.writeDouble(Double.parseDouble(set[2]));
												break;
												
									}
									
									file.seek(offsetLocations[j]+7);
									file.write(sc);

								}
							}
						}
					}catch (Exception e) {
						e.printStackTrace(System.out);
					}
			}
			}catch (Exception e) {
				e.printStackTrace(System.out);
			}
	}
}

