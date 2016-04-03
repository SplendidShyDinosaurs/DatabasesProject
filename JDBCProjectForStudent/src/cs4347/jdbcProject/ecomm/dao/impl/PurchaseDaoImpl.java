package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.SQLException;
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
			
			statement = connection.prepareStatement("INSERT INTO PURCHASE (id, prodName, prodDescription, prodCategory, prodUPC) VALUES(?,?,?,?,?);", Statement.RETURN_GENERATED_KEYS);
			statement.setString(1, purchase.getId());
			statement.setString(2, purchase.getProductID());
			statement.setDate(3, purchase.getPurchaseDate());
			statement.setString(4, purchase.getCustomerID() + "");
			statement.setString(5, purchase.getPurchaseAmount());
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
		// TODO Auto-generated method stub
		//return null;
		PreparedStatement statement = null;
		try{
			if(id == null){ throw new DAOException("Cannot retrieve a purchase with a null id"); }
			
			statement = connection.prepareStatement("SELECT id, productID, purchaseDate, customerID, purchaseAmount FROM customer where id = ?;");
			statement.setLong(1, id);
			
			ResultSet result = statement.executeQuery();
			if(!result.next()) { return null; }
			
			Purchase purchase = new Purchase();
			purchase.setId(result.getLong("id"));
			purchase.setProductID(result.getString("productID"));
			purchase.setPurchaseDate(result.getString("purchaseDate"));
			purchase.setCustomerID(result.getString("customerID"));
			purchase.setPurchaseAmount(result.getString("purchaseAmount"));
			
			return purchase;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public int update(Connection connection, Purchase purchase) throws SQLException, DAOException {
		// TODO Auto-generated method stub
		//return 0;
		PreparedStatement statement = null;
		try{
			if(purchase.getId() == null){ throw new DAOException("Cannot update a purchase with a null id"); }
			
			statement = connection.prepareStatement("UPDATE purchase SET id = ?, productID = ?, purchaseDate = ?, customerID = ?, purchaseAmount = ? WHERE id = ?;");
			statement.setString(1, purchase.getID());
			statement.setString(2, purchase.getProductID());
			statement.setDate(3, purchase.getPurchaseDate());
			statement.setString(4, purchase.getCustomerID());
			statement.setDouble(5, purchase.getPurchaseAmount());

			return statement.executeUpdate();
			
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public int delete(Connection connection, Long id) throws SQLException, DAOException {
		// TODO Auto-generated method stub
		//return 0;
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
		// TODO Auto-generated method stub
		//return null;
		PreparedStatement statement = null;
		try{
			if(customerID == null){ throw new DAOException("Cannot retrieve a purchase with a null id"); }
			
			statement = connection.prepareStatement("SELECT id, productID, purchaseDate, purchaseAmount FROM purchase where customerID = ?;");
			statement.setLong(1, customerID);
			
			ResultSet result = statement.executeQuery();
			if(!result.next()) { return null; }
			
			Purchase purchase = new Purchase();
			purchase.setID(result.getString("id"));
			purchase.setProductID(result.getString("productID"));
			purchase.setPurchaseDate(result.getString("purchaseDate"));
			purchase.setPurchaseAmount(result.getString("purchaseAmount"));
			return purchase;
			
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public List<Purchase> retrieveForProductID(Connection connection, Long productID)
			throws SQLException, DAOException {
		// TODO Auto-generated method stub
		//return null;
		PreparedStatement statement = null;
		try{
			if(productID == null){ throw new DAOException("Cannot retrieve a purchase with a null id"); }
			
			statement = connection.prepareStatement("SELECT id, productID, purchaseDate, purchaseAmount FROM purchase where productID = ?;");
			statement.setLong(1, productID);
			
			ResultSet result = statement.executeQuery();
			if(!result.next()) { return null; }
			
			Purchase purchase = new Purchase();
			purchase.setID(result.getString("id"));
			purchase.setProductID(result.getString("productID"));
			purchase.setPurchaseDate(result.getString("purchaseDate"));
			purchase.setPurchaseAmount(result.getString("purchaseAmount"));
			return purchase;
			
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public PurchaseSummary retrievePurchaseSummary(Connection connection, Long customerID)
			throws SQLException, DAOException {
		// TODO Auto-generated method stub
		//return null;
		PreparedStatement statement = null;
		try{
			if(customerID == null){ throw new DAOException("Cannot retrieve a customer with a null id"); }
			
			statement = connection.prepareStatement("SELECT id, productID, purchaseDate, purchaseAmount FROM purchase where productID = ?;");
			statement.setLong(1, productID);
			
			ResultSet result = statement.executeQuery();
			if(!result.next()) { return null; }
			
			Purchase purchase = new Purchase();
			purchase.setID(result.getString("id"));
			purchase.setProductID(result.getString("productID"));
			purchase.setPurchaseDate(result.getString("purchaseDate"));
			purchase.setPurchaseAmount(result.getString("purchaseAmount"));
			return purchase;
			
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}
	
}
