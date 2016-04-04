package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cs4347.jdbcProject.ecomm.dao.PurchaseDAO;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.entity.Customer;
import cs4347.jdbcProject.ecomm.entity.Purchase;
import cs4347.jdbcProject.ecomm.services.PurchaseSummary;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class PurchaseDaoImpl implements PurchaseDAO
{

	@Override
	public Purchase create(Connection connection, Purchase purchase) throws SQLException, DAOException {
		PreparedStatement statement = null;
		try{
			if(purchase.getId() != null){ throw new DAOException("Cannot insert a purchase with a non-null id"); }
			
			statement = connection.prepareStatement("INSERT INTO PURCHASE (productID, customerID, purchaseDate, purchaseAmount) VALUES(?,?,?,?);", Statement.RETURN_GENERATED_KEYS);
			statement.setLong(1, purchase.getProductID());
			statement.setLong(2, purchase.getCustomerID());
			statement.setDate(3, purchase.getPurchaseDate());
			statement.setDouble(4, purchase.getPurchaseAmount());
			statement.executeUpdate();
			
			ResultSet keys = statement.getGeneratedKeys();
			keys.next();
			int newKey = keys.getInt(1);
			purchase.setId((long) newKey);
			
			return purchase;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public Purchase retrieve(Connection connection, Long id) throws SQLException, DAOException {
		PreparedStatement statement = null;
		try{
			if(id == null){ throw new DAOException("Cannot retrieve a purchase with a null id"); }
			
			statement = connection.prepareStatement("SELECT id, productID, customerID, purchaseDate, purchaseAmount FROM purchase where id = ?;");
			statement.setLong(1, id);
			
			ResultSet result = statement.executeQuery();
			if(!result.next()) { return null; }
			
			Purchase purchase = new Purchase();
			purchase.setId(id);
			purchase.setProductID(result.getLong("productID"));
			purchase.setCustomerID(result.getLong("customerID"));
			purchase.setPurchaseDate(result.getDate("purchaseDate"));
			purchase.setPurchaseAmount(result.getDouble("purchaseAmount"));
			
			return purchase;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public int update(Connection connection, Purchase purchase) throws SQLException, DAOException {
		PreparedStatement statement = null;
		
		try{
			if(purchase.getId() == null){ throw new DAOException("Cannot update a purchase with a null id"); }
			
			statement = connection.prepareStatement("UPDATE purchase SET id = ?, productID = ?, customerID = ?, purchaseDate = ?, purchaseAmount = ? WHERE id = ?;");
			statement.setLong(1, purchase.getId());
			statement.setLong(2, purchase.getProductID());
			statement.setLong(3, purchase.getCustomerID());
			statement.setDate(4, purchase.getPurchaseDate());
			statement.setDouble(5, purchase.getPurchaseAmount());
			statement.setLong(6, purchase.getId());
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
			if(id == null){ throw new DAOException("Cannot delete a purchase with a null id"); }
			
			statement = connection.prepareStatement("DELETE FROM purchase WHERE ID = ?;");
			statement.setLong(1, id);

			return statement.executeUpdate();
			
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public List<Purchase> retrieveForCustomerID(Connection connection, Long customerID)
			throws SQLException, DAOException {
		PreparedStatement statement = null;
		List<Purchase> result = new ArrayList<Purchase>();
		try{
			if(customerID == null){ throw new DAOException("Cannot retrieve a purchase with a null id"); }
			statement = connection.prepareStatement("SELECT id, productID, purchaseDate, purchaseAmount FROM purchase WHERE customerID = ?;");
			statement.setLong(1, customerID);
			
			ResultSet set = statement.executeQuery();
			if(!set.next()){ return null; }
			
			while(set.next()){
			
				Purchase purchase = new Purchase();
				purchase.setId(set.getLong("id"));
				purchase.setProductID(set.getLong("productID"));
				purchase.setPurchaseDate(set.getDate("purchaseDate"));
				purchase.setPurchaseAmount(set.getDouble("purchaseAmount"));
				result.add(purchase);
				
			}
			return result;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public List<Purchase> retrieveForProductID(Connection connection, Long productID)
			throws SQLException, DAOException {
		PreparedStatement statement = null;
		List<Purchase> result = new ArrayList<Purchase>();
		try{
			if(productID == null){ throw new DAOException("Cannot retrieve a purchase with a null id"); }
			
			statement = connection.prepareStatement("SELECT id, customerID, purchaseDate, purchaseAmount FROM purchase where productID = ?;");
			statement.setLong(1, productID);
			
			ResultSet set = statement.executeQuery();
			if(!set.next()) { return null; }
			
			while(set.next()){
				
				Purchase purchase = new Purchase();
				purchase.setId(set.getLong("id"));
				purchase.setCustomerID(set.getLong("customerID"));
				purchase.setPurchaseDate(set.getDate("purchaseDate"));
				purchase.setPurchaseAmount(set.getDouble("purchaseAmount"));
				result.add(purchase);
				
			}
			return result;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public PurchaseSummary retrievePurchaseSummary(Connection connection, Long customerID)
			throws SQLException, DAOException {
		PreparedStatement statement = null;
		PurchaseSummary summary = new PurchaseSummary();
		try{
			if(customerID == null){ throw new DAOException("Cannot retrieve a purchase with a null id"); }
			
			statement = connection.prepareStatement("SELECT purchaseAmount FROM purchase where customerID = ?;");
			statement.setLong(1, customerID);
			
			ResultSet set = statement.executeQuery();
			if(!set.next()) { return null; }
			
			long total = 0;
			long current = 0;
			int numPurchases = 0;
			while(set.next()){
				current = set.getLong("purchaseAmount");
				if(current < summary.minPurchase){ summary.minPurchase = current; }
				if(current > summary.maxPurchase){ summary.maxPurchase = current; }
				numPurchases++;
				total += current;
			}
			summary.avgPurchase = (total/numPurchases);
			return summary;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}
	
}
