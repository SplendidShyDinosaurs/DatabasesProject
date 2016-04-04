package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cs4347.jdbcProject.ecomm.dao.ProductDAO;
import cs4347.jdbcProject.ecomm.entity.Product;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class ProductDaoImpl implements ProductDAO
{

	@Override
	public Product create(Connection connection, Product product) throws SQLException, DAOException {
		// TODO Auto-generated method stub
		PreparedStatement statement = null;
		try{
			if(product.getId() != null){ throw new DAOException("Cannot insert a product with a non-null id"); }
			
			statement = connection.prepareStatement("INSERT INTO PRODUCT (prodName, prodDescription, prodCategory, prodUPC) VALUES(?,?,?,?);", Statement.RETURN_GENERATED_KEYS);
			statement.setString(1, product.getProdName());
			statement.setString(2, product.getProdDescription());
			statement.setInt(3, product.getProdCategory());
			statement.setString(4, product.getProdUPC());
			statement.executeUpdate();
			
			ResultSet keys = statement.getGeneratedKeys();
			keys.next();
			int newKey = keys.getInt(1);
			product.setId((long) newKey);
			
			return product;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public Product retrieve(Connection connection, Long id) throws SQLException, DAOException {
		PreparedStatement statement = null;
		try{
			if(id == null){ throw new DAOException("Cannot retrieve a product with a null id"); }
			
			statement = connection.prepareStatement("SELECT id, prodName, prodDescription, prodCategory, prodUPC FROM product where id = ?;");
			statement.setLong(1, id);
			
			ResultSet result = statement.executeQuery();
			if(!result.next()) { return null; }
			
			Product product = new Product();
			product.setId(result.getLong("id"));
			product.setProdName(result.getString("prodName"));
			product.setProdDescription(result.getString("prodDescription"));
			product.setProdCategory(result.getInt("prodCategory"));
			product.setProdUPC(result.getString("prodUPC"));
		
			return product;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public int update(Connection connection, Product product) throws SQLException, DAOException {
		PreparedStatement statement = null;
		try{
			if(product.getId() == null){ throw new DAOException("Cannot update a product with a null id"); }
			
			statement = connection.prepareStatement("UPDATE product SET prodName = ?, prodDescription = ?, prodCategory = ?, prodUPC = ? WHERE id = ?;");
			statement.setString(1, product.getProdName());
			statement.setString(2, product.getProdDescription());
			statement.setInt(3, product.getProdCategory());
			statement.setString(4, product.getProdUPC());
			statement.setLong(5, product.getId());

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
			if(id == null){ throw new DAOException("Cannot delete a product with a null id"); }
			
			statement = connection.prepareStatement("DELETE FROM product WHERE ID = ?;");
			statement.setLong(1, id);

			return statement.executeUpdate();
			
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public List<Product> retrieveByCategory(Connection connection, int category) throws SQLException, DAOException {
		PreparedStatement statement = null;
		List<Product> result = new ArrayList<Product>();
		try{
			if(category < 0){ throw new DAOException("Cannot retrieve a product with a "); }
		
			statement = connection.prepareStatement("SELECT id, prodName, prodDescription, prodCategory, prodUPC FROM product where prodCategory = ?;");
			statement.setInt(1, category);
			ResultSet set = statement.executeQuery();
			
			while(set.next()) {
			
				Product product = new Product();
				product.setId(set.getLong("id"));
				product.setProdName(set.getString("ProdName"));
				product.setProdDescription(set.getString("ProdDescription"));
				product.setProdCategory(set.getInt("ProdCategory"));
				product.setProdUPC(set.getString("ProdUPC"));
				result.add(product);
			}
			
			return result;
		}catch(SQLException e){
			throw new DAOException(e.getMessage());
		}finally{
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

	@Override
	public Product retrieveByUPC(Connection connection, String upc) throws SQLException, DAOException {
		if (upc == null) { throw new DAOException("Trying to retrieve Product with a UPC that does not exist"); }
		
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement("SELECT id, prodName, prodDescription, prodCategory FROM product WHERE prodUPC = ?;");
			statement.setString(1, upc);
			ResultSet result = statement.executeQuery();
			if(!result.next()) { return null; }
			
			Product prod = new Product();
			prod.setProdName((result.getString("prodName")));
			prod.setProdDescription(result.getString("prodDescription"));
			prod.setProdCategory(result.getInt("prodCategory"));
			prod.setProdUPC(result.getString("prodUPC"));
			prod.setId(result.getLong("id"));
			return prod;
		}
		finally {
			if(statement != null && !statement.isClosed()){ statement.close(); }
		}
	}

}
