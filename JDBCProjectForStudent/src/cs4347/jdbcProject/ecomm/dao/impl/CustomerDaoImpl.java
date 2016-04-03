package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cs4347.jdbcProject.ecomm.dao.CustomerDAO;
import cs4347.jdbcProject.ecomm.entity.Customer;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class CustomerDaoImpl implements CustomerDAO
{

	@Override
	public Customer create(Connection connection, Customer customer) throws SQLException, DAOException {
		PreparedStatement statement = null;
		try{
			if(customer.getId() != null){ throw new DAOException("Cannot insert a customer with a non-null id"); }
			
			statement = connection.prepareStatement("INSERT INTO CUSTOMER (firstName, lastName, dob, gender, email) VALUES(?,?,?,?,?);", Statement.RETURN_GENERATED_KEYS);
			statement.setString(1, customer.getFirstName());
			statement.setString(2, customer.getLastName());
			statement.setDate(3, customer.getDob());
			statement.setString(4, customer.getGender() + "");
			statement.setString(5, customer.getEmail());
			statement.executeUpdate();
			
			ResultSet keys = statement.getGeneratedKeys();
			keys.next();
			int newKey = keys.getInt(1);
			customer.setId((long) newKey);
			
			return customer;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public Customer retrieve(Connection connection, Long id) throws SQLException, DAOException {
		PreparedStatement statement = null;
		try{
			if(id == null){ throw new DAOException("Cannot retrieve a customer with a null id"); }
			
			statement = connection.prepareStatement("SELECT id, firstName, lastName, gender, dob, email FROM customer where id = ?;");
			statement.setLong(1, id);
			
			ResultSet result = statement.executeQuery();
			if(!result.next()) { return null; }
			
			Customer customer = new Customer();
			customer.setId(result.getLong("id"));
			customer.setFirstName(result.getString("firstName"));
			customer.setLastName(result.getString("lastName"));
			customer.setGender(result.getString("gender").charAt(0));
			customer.setDob(result.getDate("dob"));
			customer.setEmail(result.getString("email"));
			
			return customer;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public int update(Connection connection, Customer customer) throws SQLException, DAOException {
		PreparedStatement statement = null;
		try{
			if(customer.getId() == null){ throw new DAOException("Cannot update a customer with a null id"); }
			
			statement = connection.prepareStatement("UPDATE customer SET firstName = ?, lastName = ?, gender = ?, dob = ?, email = ? WHERE id = ?;");
			statement.setString(1, customer.getFirstName());
			statement.setString(2, customer.getLastName());
			statement.setString(3, String.valueOf(customer.getGender()));
			statement.setDate(4, customer.getDob());
			statement.setString(5, customer.getEmail());
			statement.setLong(6, customer.getId());

			return statement.executeUpdate();
			
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public int delete(Connection connection, Long id) throws SQLException, DAOException {
		PreparedStatement statement = null;
		try{
			if(id == null){ throw new DAOException("Cannot delete a customer with a null id"); }
			
			statement = connection.prepareStatement("DELETE FROM customer WHERE ID = ?;");
			statement.setLong(1, id);

			return statement.executeUpdate();
			
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public List<Customer> retrieveByZipCode(Connection connection, String zipCode) throws SQLException, DAOException {
		PreparedStatement statement = null;
		List<Customer> result = new ArrayList<Customer>();
		try{
			if(zipCode == null){ throw new DAOException("Cannot retrieve a customer with a null zipcode"); }
		
			statement = connection.prepareStatement("SELECT id, firstName, lastName, gender, dob, email FROM customer where zipcode = ?;");
			statement.setString(1, zipCode);
			ResultSet set = statement.executeQuery();
			
			while(set.next()) {
			
				Customer customer = new Customer();
				customer.setId(set.getLong("id"));
				customer.setFirstName(set.getString("firstName"));
				customer.setLastName(set.getString("lastName"));
				customer.setGender(set.getString("gender").charAt(0));
				customer.setDob(set.getDate("dob"));
				customer.setEmail(set.getString("email"));
				result.add(customer);
			}
			
			return result;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public List<Customer> retrieveByDOB(Connection connection, Date startDate, Date endDate)
			throws SQLException, DAOException {
		PreparedStatement statement = null;
		List<Customer> result = new ArrayList<Customer>();
		try{
			if(startDate == null || endDate == null){ throw new DAOException("Cannot retrieve a customer with a null date"); }
		
			statement = connection.prepareStatement("SELECT id, firstName, lastName, gender, email FROM customer where dob >= ? AND dob <= ?;");
			statement.setDate(1, startDate);
			statement.setDate(2, endDate);
			ResultSet set = statement.executeQuery();
			
			while(set.next()) {
			
				Customer customer = new Customer();
				customer.setId(set.getLong("id"));
				customer.setFirstName(set.getString("firstName"));
				customer.setLastName(set.getString("lastName"));
				customer.setGender(set.getString("gender").charAt(0));
				customer.setDob(set.getDate("dob"));
				customer.setEmail(set.getString("email"));
				result.add(customer);
			}
			
			return result;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}
	
}
