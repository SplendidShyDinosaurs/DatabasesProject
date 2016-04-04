package cs4347.jdbcProject.ecomm.services.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.dao.PurchaseDAO;
import cs4347.jdbcProject.ecomm.dao.impl.PurchaseDaoImpl;
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

		PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try{
			connection.setAutoCommit(false);
			Purchase p = purchaseDAO.create(connection, purchase);
			connection.commit();
			return	p;
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
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			Purchase purchase = purchaseDAO.retrieve(connection, id);
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
		PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
		Connection connection = dataSource.getConnection();
		try {
			connection.setAutoCommit(false);
			int value = purchaseDAO.delete(connection, id);
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
		PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			List<Purchase> list = purchaseDAO.retrieveForCustomerID(connection, customerID);
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
	public PurchaseSummary retrievePurchaseSummary(Long customerID) throws SQLException, DAOException {
		PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			PurchaseSummary summary = purchaseDAO.retrievePurchaseSummary(connection, customerID);
			connection.commit();
			return summary;
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
		PurchaseDAO purchaseDAO = new PurchaseDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			List<Purchase> list = purchaseDAO.retrieveForProductID(connection, productID);
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