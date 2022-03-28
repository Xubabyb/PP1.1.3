package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        String query = "CREATE TABLE IF NOT EXISTS users " +
                "( id BIGINT PRIMARY KEY AUTO_INCREMENT,name VARCHAR (30),lastname VARCHAR (30),age TINYINT)";
        try (PreparedStatement preparedStatement = Util.getConnection().prepareStatement(query)) {
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();

        }
    }

    public void dropUsersTable() {
        String query = "DROP TABLE IF EXISTS users";
        try (PreparedStatement preparedStatement = Util.getConnection().prepareStatement(query)) {
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String query = "INSERT INTO users (name, lastname, age) VALUE (?,?,?)";
        try (PreparedStatement preparedStatement = Util.getConnection().prepareStatement(query)) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setString(3, String.valueOf(age));
            preparedStatement.executeUpdate();
            System.out.printf("User named %s added to table\n", name);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) {
        String query = "DELETE FROM users WHERE id = ?";
        try (PreparedStatement preparedStatement = Util.getConnection().prepareStatement(query)) {
            /* DELETE - является оператором DML (Data Manipulation Language) - модет удалить часть данных
             * из таблицы в соответствии с условием WHERE.
             * Операторы DDL управляют структурой, а операторы DML - её содержимым*/
            preparedStatement.setString(1, String.valueOf(id));
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        String query = "SELECT * FROM users";
        try (PreparedStatement preparedStatement = Util.getConnection().prepareStatement(query)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                String lastname = resultSet.getString("lastname");
                Byte age = resultSet.getByte("age");

                User temp = new User(name, lastname, age);
                temp.setId(id);
                users.add(temp);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return users;
    }

    public void cleanUsersTable() {
        String query = "TRUNCATE users";
        try (PreparedStatement preparedStatement = Util.getConnection().prepareStatement(query)) {
            /* TRUNCATE - является оператором DDL (Data Definition Language) - удаляет все данные из таблицы
             * Операторы DDL управляют структурой, а операторы DML - её содержимым*/
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
