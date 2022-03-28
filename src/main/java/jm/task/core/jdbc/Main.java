package jm.task.core.jdbc;

import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

public class Main {
    public static void main(String[] args) {
        UserService userService = new UserServiceImpl();
        userService.createUsersTable();
        userService.saveUser("Vasiliy", "Boom", (byte) 39);
        userService.saveUser("Natasha", "Bum", (byte) 35);
        userService.saveUser("David", "BoomBum", (byte) 14);
        userService.saveUser("Valera", "Bull", (byte) 49);
        System.out.println(userService.getAllUsers());
        userService.cleanUsersTable();
        userService.dropUsersTable();
    }
}
