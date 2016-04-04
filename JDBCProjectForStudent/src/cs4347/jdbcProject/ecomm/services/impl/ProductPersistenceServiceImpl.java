package cs4347.jdbcProject.ecomm.services.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.dao.ProductDAO;
import cs4347.jdbcProject.ecomm.dao.impl.ProductDaoImpl;
import cs4347.jdbcProject.ecomm.entity.Product;
import cs4347.jdbcProject.ecomm.services.ProductPersistenceService;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class ProductPersistenceServiceImpl implements ProductPersistenceService
{
	private DataSource dataSource;

	public ProductPersistenceServiceImpl(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}

	@Override
	public Product create(Product product) throws SQLException, DAOException {
		// TODO Auto-generated method stub
		ProductDAO ProductDAO = new ProductDaoImpl();

		Connection connection = dataSource.getConnection();
		try {
			connection.setAutoCommit(false);
			Product prod = ProductDAO.create(connection, product);
			//Long prodID = prod.getId();
			connection.commit();
			return prod;
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
	public Product retrieve(Long id) throws SQLException, DAOException {
		// TODO Auto-generated method stub
		ProductDAO ProductDAO = new ProductDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			Product prod = ProductDAO.retrieve(connection, id);
			connection.commit();
			return prod;
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
	public int update(Product product) throws SQLException, DAOException {
		// TODO Auto-generated method stub
		ProductDAO ProductDAO = new ProductDaoImpl();

		Connection connection = dataSource.getConnection();
		try {
			connection.setAutoCommit(false);
			int prod = ProductDAO.update(connection, product);
			connection.commit();
			return prod;
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
		// TODO Auto-generated method stub
		ProductDAO ProductDAO = new ProductDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			int prod = ProductDAO.delete(connection, id);
			connection.commit();
			return prod;
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
	public Product retrieveByUPC(String upc) throws SQLException, DAOException {
		ProductDAO productDAO = new ProductDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			Product prod = productDAO.retrieveByUPC(connection, upc);
			connection.commit();
			return prod;
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
	public List<Product> retrieveByCategory(int category) throws SQLException, DAOException {
		// TODO Auto-generated method stub
		ProductDAO ProductDAO = new ProductDaoImpl();
		Connection connection = dataSource.getConnection();
		
		try {
			connection.setAutoCommit(false);
			List<Product> ProdList = ProductDAO.retrieveByCategory(connection, category);
			connection.commit();
			return ProdList;
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