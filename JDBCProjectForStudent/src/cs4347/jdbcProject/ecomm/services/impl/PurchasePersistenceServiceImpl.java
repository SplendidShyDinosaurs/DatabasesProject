package cs4347.jdbcProject.ecomm.services.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.dao.AddressDAO;
import cs4347.jdbcProject.ecomm.dao.CreditCardDAO;
import cs4347.jdbcProject.ecomm.dao.CustomerDAO;
import cs4347.jdbcProject.ecomm.dao.ProductDAO;
import cs4347.jdbcProject.ecomm.dao.PurchaseDAO;
import cs4347.jdbcProject.ecomm.dao.impl.AddressDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.CreditCardDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.CustomerDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.ProductDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.PurchaseDaoImpl;
import cs4347.jdbcProject.ecomm.entity.Address;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.entity.Customer;
import cs4347.jdbcProject.ecomm.entity.Product;
import cs4347.jdbcProject.ecomm.entity.Purchase;
import cs4347.jdbcProject.ecomm.services.PurchasePersistenceService;
import cs4347.jdbcProject.ecomm.services.PurchaseSummary;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class PurchasePersistenceServiceImpl implements PurchasePersistenceService
{
	private DataSource dataSource;

	public PurchasePersistenceServiceImpl(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}

	@Override
	public Purchase create(Purchase purchase) throws SQLException, DAOException {
		// TODO Auto-generated method stub
		//return null;
		PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

		Connection connection = dataSource.getConnection();
		try {
			connection.setAutoCommit(false);
			Purchase purch = purchaseDAO.create(connection, purchase);
			Long purchID = purch.getId();

			//if (cust.getAddress() == null) {
				//throw new DAOException("Customers must include an Address instance.");
			//}
			//Address address = purch.getAddress();
			//addressDAO.create(connection, address, purchID);

			//if (purch.getCreditCard() == null) {
			//	throw new DAOException("Customers must include a CreditCard instance.");
			//}
			//CreditCard creditCard = cust.getCreditCard();
			//creditCardDAO.create(connection, creditCard, custID);

			connection.commit();
			return purch;
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
	public Purchase retrieve(Long id) throws SQLException, DAOException {
		PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		CreditCardDAO creditCardDAO = new CreditCardDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			Purchase purchase = purchaseDAO.retrieve(connection, id);
			addressDAO.retrieveForCustomerID(connection, id);
			creditCardDAO.retrieveForCustomerID(connection, id);
			connection.commit();
			return purchase;
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
	public int update(Purchase purchase) throws SQLException, DAOException {
		PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			int numUpdates = purchaseDAO.update(connection, purchase);
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
		PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

		Connection connection = dataSource.getConnection();
		try {
			connection.setAutoCommit(false);
			int value = purchaseDAO.delete(connection, id);
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
	public List<Purchase> retrieveForCustomerID(Long customerID) throws SQLException, DAOException {
		PurchaseDAO purchaseDAO = new purchaseDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			List<Purchase> purchList = PurchaseDAO.retrieveForCustomerID(connection, customerID);
			connection.commit();
			return purchlist;
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
	public PurchaseSummary retrievePurchaseSummary(Long customerID) throws SQLException, DAOException {
		PurchaseDAO purchaseDAO = new purchaseDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			List<Purchase> purchList = PurchaseDAO.retrievePurchaseSummary(connection, customerID);
			connection.commit();
			return purchlist;
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
	public List<Purchase> retrieveForProductID(Long productID) throws SQLException, DAOException {
		PurchaseDAO purchaseDAO = new purchaseDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			List<Purchase> purchList = PurchaseDAO.retrieveForProductID(connection, productID);
			connection.commit();
			return purchlist;
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
