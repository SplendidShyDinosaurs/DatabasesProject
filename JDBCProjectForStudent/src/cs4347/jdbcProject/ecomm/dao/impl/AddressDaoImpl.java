package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

//import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.dao.AddressDAO;
import cs4347.jdbcProject.ecomm.entity.Address;
//import cs4347.jdbcProject.ecomm.testing.DataSourceManager;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class AddressDaoImpl implements AddressDAO
{

	@Override
	public Address create(Connection connection, Address address, Long customerID) throws SQLException, DAOException {		
		PreparedStatement statement = null;
		try{
			if(customerID == null){ throw new DAOException("Cannot insert an address with a null id"); }
			
			statement = connection.prepareStatement("INSERT INTO ADDRESS (address1, address2, city, state, zipcode, customerID) VALUES(?,?,?,?,?,?);");
			statement.setString(1, address.getAddress1());
			statement.setString(2, address.getAddress2());
			statement.setString(3, address.getCity());
			statement.setString(4, address.getState());
			statement.setString(5, address.getZipcode());
			statement.setLong(6, customerID);
			statement.executeUpdate();
			return address;
			
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public Address retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		if (customerID == null) {
			throw new DAOException("Trying to retrieve Address of with a customer ID that does not exist");
		}
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement("SELECT address1, address2, city, state, zipcode FROM address where customerID = ?");
			statement.setLong(1, customerID);
			ResultSet result = statement.executeQuery();
			if(!result.next()) { return null; }
			
			Address addr = new Address();
			addr.setAddress1((result.getString("address1")));
			addr.setAddress2(result.getString("address2"));
			addr.setCity(result.getString("city"));
			addr.setState(result.getString("state"));
			addr.setZipcode(result.getString("zipcode"));
			return addr;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally {
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		if (customerID == null) { throw new DAOException("Trying to delete Address with NULL ID"); }
		
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement("DELETE FROM CUSTOMER WHERE ID = ?;");
			statement.setLong(1, customerID);
			statement.executeUpdate();
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally {
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
		
	}

}
