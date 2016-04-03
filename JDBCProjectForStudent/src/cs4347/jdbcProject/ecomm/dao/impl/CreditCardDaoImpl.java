package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import cs4347.jdbcProject.ecomm.dao.CreditCardDAO;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.entity.Customer;
import cs4347.jdbcProject.ecomm.util.DAOException;


public class CreditCardDaoImpl implements CreditCardDAO
{

	@Override
	public CreditCard create(Connection connection, CreditCard creditCard, Long customerID)
			throws SQLException, DAOException {
		PreparedStatement statement = null;
		try{
			if(customerID == null){ throw new DAOException("Cannot insert a credit card with a null id"); }
			
			statement = connection.prepareStatement("INSERT INTO creditcard (name, ccNumber, expDate, securityCode, customerID) VALUES(?,?,?,?,?);");
			statement.setString(1, creditCard.getName());
			statement.setString(2, creditCard.getCcNumber());
			statement.setString(3, creditCard.getExpDate());
			statement.setString(4, creditCard.getSecurityCode());
			statement.setLong(5, customerID);
			statement.executeUpdate();
			
			return creditCard;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public CreditCard retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		PreparedStatement statement = null;
		try{
			if(customerID == null){ throw new DAOException("Cannot retrieve a credit card with a null id"); }
			
			statement = connection.prepareStatement("SELECT name, ccNumber, expDate, securityCode FROM creditcard where customerID = ?;");
			statement.setLong(1, customerID);
			
			ResultSet result = statement.executeQuery();
			if(!result.next()) { return null; }
			
			CreditCard card = new CreditCard();
			card.setName(result.getString("name"));
			card.setCcNumber(result.getString("ccNumber"));
			card.setExpDate(result.getString("expDate"));
			card.setSecurityCode(result.getString("securityCode"));
			return card;
			
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		PreparedStatement statement = null;
		try{
			if(customerID == null){ throw new DAOException("Cannot delete a credit card with a null id"); }
			
			statement = connection.prepareStatement("DELETE FROM creditcard WHERE customerID = ?;");
			statement.setLong(1, customerID);
			statement.executeUpdate();
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}	
	}

}
