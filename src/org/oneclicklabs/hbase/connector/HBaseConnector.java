package org.oneclicklabs.hbase.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class HBaseConnector {

	private static Configuration configuration = null;
	private static HTable hTable;
	private static HBaseAdmin admin;
	private static Logger logger = Logger.getLogger(HBaseConnector.class);

	/**
	 * 
	 */
	static {
		configuration = HBaseConfiguration.create();
	}

	/**
	 * 
	 * @param tableName
	 * @param familys
	 * @throws Exception
	 */
	public static void creatTable(String tableName, String[] familys)
			throws Exception {

		admin = new HBaseAdmin(configuration);
		if (admin.tableExists(tableName)) {

			logger.debug("Table Already Exists!");

		} else {

			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			logger.debug(tableName + " table created successfully !! ");

		}

		admin.close();
	}

	/**
	 * 
	 * @param tableName
	 * @throws Exception
	 */
	public static void deleteTable(String tableName) throws Exception {

		try {

			admin = new HBaseAdmin(configuration);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);

			logger.debug(tableName + " table deleteed successfully");
			admin.close();

		} catch (MasterNotRunningException e) {
			logger.error(e.getMessage());

		} catch (ZooKeeperConnectionException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 * @throws Exception
	 */
	public static void addRecord(String tableName, String rowKey,
			String family, String qualifier, String value) throws Exception {
		try {
			hTable = new HTable(configuration, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			hTable.put(put);
			logger.debug("insert recored " + rowKey + " to table " + tableName
					+ " successfully");

			hTable.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param rowKey
	 * @throws IOException
	 */
	public static void delRecord(String tableName, String rowKey)
			throws IOException {
		hTable = new HTable(configuration, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		hTable.delete(list);
		logger.debug(rowKey + " record deleteed successfully");
		hTable.close();
	}

	/**
	 * 
	 * @param tableName
	 * @param rowKey
	 * @throws IOException
	 */
	public static void getOneRecord(String tableName, String rowKey)
			throws IOException {
		hTable = new HTable(configuration, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = hTable.get(get);
		for (KeyValue kv : rs.raw()) {

			StringBuffer buffer = new StringBuffer();
			buffer.append(new String(kv.getRow()) + " ");
			buffer.append(new String(kv.getFamily()) + ":");
			buffer.append(new String(kv.getQualifier()) + " ");
			buffer.append(kv.getTimestamp() + " ");
			logger.debug(buffer);
			logger.debug(new String(kv.getValue()));
		}
		hTable.close();
	}

	/**
	 * 
	 * @param tableName
	 */
	public static void getAllRecord(String tableName) {
		try {
			hTable = new HTable(configuration, tableName);
			Scan s = new Scan();
			ResultScanner ss = hTable.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					StringBuffer buffer = new StringBuffer();
					buffer.append(new String(kv.getRow()) + " ");
					buffer.append(new String(kv.getFamily()) + ":");
					buffer.append(new String(kv.getQualifier()) + " ");
					buffer.append(kv.getTimestamp() + " ");
					logger.debug(buffer);
					logger.debug(new String(kv.getValue()));
				}
			}
			hTable.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] agrs) {
		try {
			String tablename = "scores";
			String[] familys = { "grade", "course" };
			HBaseConnector.creatTable(tablename, familys);

			// add record zkb
			HBaseConnector.addRecord(tablename, "zkb", "grade", "", "5");
			HBaseConnector.addRecord(tablename, "zkb", "course", "", "90");
			HBaseConnector.addRecord(tablename, "zkb", "course", "math", "97");
			HBaseConnector.addRecord(tablename, "zkb", "course", "art", "87");
			// add record baoniu
			HBaseConnector.addRecord(tablename, "baoniu", "grade", "", "4");
			HBaseConnector.addRecord(tablename, "baoniu", "course", "math",
					"89");

			logger.debug("===========get one record========");
			HBaseConnector.getOneRecord(tablename, "zkb");

			logger.debug("===========show all record========");
			HBaseConnector.getAllRecord(tablename);

			logger.debug("===========del one record========");
			HBaseConnector.delRecord(tablename, "baoniu");
			HBaseConnector.getAllRecord(tablename);

			logger.debug("===========show all record========");
			HBaseConnector.getAllRecord(tablename);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}