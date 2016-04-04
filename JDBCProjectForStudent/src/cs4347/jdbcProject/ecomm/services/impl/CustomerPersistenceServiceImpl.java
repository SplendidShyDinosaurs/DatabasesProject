package cs4347.jdbcProject.ecomm.services.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.dao.AddressDAO;
import cs4347.jdbcProject.ecomm.dao.CreditCardDAO;
import cs4347.jdbcProject.ecomm.dao.CustomerDAO;
import cs4347.jdbcProject.ecomm.dao.impl.AddressDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.CreditCardDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.CustomerDaoImpl;
import cs4347.jdbcProject.ecomm.entity.Address;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.entity.Customer;
import cs4347.jdbcProject.ecomm.services.CustomerPersistenceService;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class CustomerPersistenceServiceImpl implements CustomerPersistenceService
{
	private DataSource dataSource;

	public CustomerPersistenceServiceImpl(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}
	
	/**
	 * This method provided as an example of transaction support across multiple inserts.
	 * 
	 * Persists a new Customer instance by inserting new Customer, Address, 
	 * and CreditCard instances. Notice the transactional nature of this 
	 * method which includes turning off autocommit at the start of the 
	 * process, and rolling back the transaction if an exception 
	 * is caught. 
	 */
	@Override
	public Customer create(Customer customer) throws SQLException, DAOException
	{
		CustomerDAO customerDAO = new CustomerDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

		Connection connection = dataSource.getConnection();
		try {
			connection.setAutoCommit(false);
			Customer cust = customerDAO.create(connection, customer);
			Long custID = cust.getId();

			if (cust.getAddress() == null) {
				throw new DAOException("Customers must include an Address instance.");
			}
			Address address = cust.getAddress();
			addressDAO.create(connection, address, custID);

			if (cust.getCreditCard() == null) {
				throw new DAOException("Customers must include an CreditCard instance.");
			}
			CreditCard creditCard = cust.getCreditCard();
			creditCardDAO.create(connection, creditCard, custID);

			connection.commit();
			return cust;
		}
		catch (Exception ex) {
			connection.rollback();
			throw ex;
		}
		finally {
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	@Override
	public Customer retrieve(Long id) throws SQLException, DAOException {
		CustomerDAO customerDAO = new CustomerDaoImpl();
		CreditCardDAO creditcardDAO = new CreditCardDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			Customer cust = customerDAO.retrieve(connection, id);
			Address address = addressDAO.retrieveForCustomerID(connection, id);
			CreditCard creditCard = creditcardDAO.retrieveForCustomerID(connection, id);
			
			if (address == null) { throw new DAOException("Customers need an Address instance."); }
			else{ cust.setAddress(address); }

			if (creditCard == null) { throw new DAOException("Customers need aa CreditCard instance."); }
			else { cust.setCreditCard(creditCard); }
			
			return cust;
		}
		catch (Exception ex) {
			connection.rollback();
			throw ex;
		}
		finally {
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	@Override
	public int update(Customer customer) throws SQLException, DAOException {
		CustomerDAO customerDAO = new CustomerDaoImpl();
		Connection connection = dataSource.getConnection();
		Address address = new Address();
		CreditCard card = new CreditCard();
		PreparedStatement statement1 = null;
		PreparedStatement statement2 = null;
		
		try {
			connection.setAutoCommit(false);
			int numUpdates = customerDAO.update(connection, customer);
			statement1 = connection.prepareStatement("UPDATE address SET address1 = ?, address2= ?, city = ?, state = ?, zipcode = ? WHERE customerID = ?;");
			statement2 = connection.prepareStatement("UPDATE creditcard SET name = ?, ccNumber = ?, expDate = ?, securityCode = ? WHERE customerID = ?;");
			
			//Update address
			statement1.setString(1, address.getAddress1());
			statement1.setString(2, address.getAddress2());
			statement1.setString(3, address.getCity());
			statement1.setString(4, address.getState());
			statement1.setString(5, address.getZipcode());
			statement1.setLong(6, customer.getId());
			statement1.executeUpdate();
			
			//update credit card
			statement2.setString(1, card.getName());
			statement2.setString(2, card.getCcNumber());
			statement2.setString(3, card.getExpDate());
			statement2.setString(4, card.getSecurityCode());
			statement2.setLong(5, customer.getId());
			statement2.executeUpdate();
			
			connection.commit();
			return numUpdates;
		}
		catch (Exception ex) {
			connection.rollback();
			throw ex;
		}
		finally {
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	@Override
	public int delete(Long id) throws SQLException, DAOException {
		CustomerDAO customerDAO = new CustomerDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

		Connection connection = dataSource.getConnection();
		try {
			connection.setAutoCommit(false);
			int value = customerDAO.delete(connection, id);
			addressDAO.deleteForCustomerID(connection, id);
		    creditCardDAO.deleteForCustomerID(connection, id);
			connection.commit();
			return value;
		}
		catch (Exception ex) {
			connection.rollback();
			throw ex;
		}
		finally {
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	@Override
	public List<Customer> retrieveByZipCode(String zipCode) throws SQLException, DAOException {
		CustomerDAO customerDAO = new CustomerDaoImpl();
		Connection connection = dataSource.getConnection();
		try {
			connection.setAutoCommit(false);
			List<Customer> list = customerDAO.retrieveByZipCode(connection, zipCode);
			connection.commit();
			return list;
		}
		catch (Exception ex) {
			connection.rollback();
			throw ex;
		}
		finally {
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	@Override
	public List<Customer> retrieveByDOB(Date startDate, Date endDate) throws SQLException, DAOException {
		CustomerDAO customerDAO = new CustomerDaoImpl();
		Connection connection = dataSource.getConnection();
		try {
			connection.setAutoCommit(false);
			List<Customer> list = customerDAO.retrieveByDOB(connection, startDate, endDate);
			connection.commit();
			return list;
		}
		catch (Exception ex) {
			connection.rollback();
			throw ex;
		}
		finally {
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}
}
