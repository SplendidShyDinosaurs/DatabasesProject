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
		String selectQuery = "SELECT address1, address2, city, state, zipCode FROM address where customerID = ?";
		PreparedStatement ps = null;
		try {
			ps = connection.prepareStatement(selectQuery);
			ps.setLong(1, customerID);
			ResultSet rs = ps.executeQuery();
			if(!rs.next()) {
				return null;
			}
			
			Address addr = new Address();
			addr.setAddress1((rs.getString("address1")));
			addr.setAddress2(rs.getString("address2"));
			addr.setCity(rs.getString("city"));
			addr.setState(rs.getString("state"));
			addr.setZipcode(rs.getString("zipcode"));
			return addr;
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	@Override
	public void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		if (customerID == null) {
			throw new DAOException("Trying to delete Address with NULL ID");
		}
		PreparedStatement ps = null;
		String deleteSQL = "DELETE FROM CUSTOMER WHERE ID = ?;";
		try {
			ps = connection.prepareStatement(deleteSQL);
			ps.setLong(1, customerID);

			int rows = ps.executeUpdate();
			/* There was a return statement here, but because the function is void, it was deleted*/
		}
		finally {
			if (ps != null && !ps.isClosed()) {
				ps.close();
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
		
	}

}
