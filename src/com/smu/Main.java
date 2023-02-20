package com.smu;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * com.smu.Main
 *
 * @author T.W 12/2/22
 */
public class Main {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Please check your running command!");
        } else {
            //get items number of db and files name from command
            int itemsNumInDB = Integer.parseInt(args[0]);
            //put all filenames into a array
            List<String> fileNames = new ArrayList<>(Arrays.asList(args).subList(1, args.length));
            //initialize the com.smu.Database and com.smu.LockManager
            Database db = new Database(itemsNumInDB, false);
            LockManager lockManager = new LockManager();
            Random random = new Random();
            List<Transaction> transactions = new ArrayList<>();
            for (String fileName : fileNames) {
                //Read each file and as it a transaction
                List<String> commandsLine = readFile(fileName);
                if (commandsLine.isEmpty()) {
                    continue;
                }
                String[] firstLineChars = commandsLine.get(0).split(" ");
                int instructionsNum = Integer.parseInt(String.valueOf(firstLineChars[0]));
                int localVariablesNum = Integer.parseInt(String.valueOf(firstLineChars[1]));
                if (localVariablesNum < 0 || instructionsNum < 0 || commandsLine.size() != instructionsNum + 1) {
                    System.out.println("Please check file: " + fileName);
                    continue;
                }
                Transaction transaction = new Transaction(localVariablesNum);
                transaction.setCommandLines(commandsLine);
                transactions.add(transaction);
            }
            if (transactions.isEmpty()) {
                System.out.println("Please check your files!");
                System.exit(1);
            }
            int order = 0;
            while (!transactions.isEmpty()) {
                int index = random.nextInt(transactions.size());
                Transaction randTransaction = transactions.get(index);
                if (randTransaction.getCommandLines().size() <= 1) {
                    //Kick this transaction out
                    transactions.remove(index);
                    continue;
                }
                if (null == randTransaction.getTid()) {
                    //Initialize the tid as the order of transaction being executed
                    randTransaction.setTid(order);
                    //Initialize the database old values for rollback
                    List<RollbackLog> rollbackLogs = new ArrayList<>();
                    for (int i = 0; i < itemsNumInDB; i++) {
                        rollbackLogs.add(new RollbackLog(i, db.read(i)));
                    }
                    randTransaction.setRollbackLogs(rollbackLogs);
                    order++;
                }
                int result = executeCommands(lockManager, randTransaction, db);
                if (result == 0) {
                    //this transaction has rolled back
                    //Release all locks
                    lockManager.releaseAll(randTransaction.getTid());
                    //Kick this transaction out
                    transactions.remove(index);
                }
            }
            db.print();
            System.exit(1);
        }
    }

    private static List<String> readFile(String fileName) {
        String path = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(fileName)).getPath();
        List<String> results = new ArrayList<>();
        //read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            results = stream.collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    private static int executeCommands(LockManager lockManager, Transaction transaction, Database db) {
        List<String> commandLines = transaction.getCommandLines();
        int tid = transaction.getTid();
        //always select the first command (not the first line) to execute
        String[] instruction = commandLines.get(1).split(" ");
        String operation = instruction[0];
        int operand1 = Integer.parseInt(instruction[1]);
        int operand2 = Integer.parseInt(instruction[2]);
        int currentCommandId = transaction.getCurrentCommandId();
        System.out.println("T" + tid + " execute " + operation + operand1 + operand2 + " " + currentCommandId);
        switch (operation) {
            case "R":
                //read db[x] and store it to local[y]
                int requestSLock = lockManager.request(tid, operand1, true);
                if (requestSLock == 1) {
                    transaction.read(db, operand1, operand2);
//                    System.out.println("T" + tid + " local variables are " + transaction.getLocal());
//                    db.print();
                    //after executing one command, remove it from commandLines
                    commandLines.remove(1);
                    transaction.setCommandLines(commandLines);
                    transaction.setCurrentCommandId(currentCommandId + 1);
                } else if (requestSLock == 0) {
                    transaction.setBlocked(true);
//                    System.out.println("T" + tid + " local variables are " + transaction.getLocal());
//                    db.print();
                } else {
                    rollback(transaction, db);
//                    System.out.println("T" + tid + " local variables are " + transaction.getLocal());
//                    db.print();
                    return 0;
                }
                checkFinishStatue(transaction,lockManager);
                return 1;
            case "W":
                //write local[x] to db[y]
                int requestXLock = lockManager.request(tid, operand2, false);
                if (requestXLock == 1) {
                    transaction.write(db, operand1, operand2);
//                    System.out.println("T" + tid + " local variables are " + transaction.getLocal());
//                    db.print();
                    //after executing one command, remove it from commandLines
                    commandLines.remove(1);
                    transaction.setCommandLines(commandLines);
                    transaction.setCurrentCommandId(currentCommandId + 1);
                } else if (requestXLock == 0) {
                    transaction.setBlocked(true);
//                    System.out.println("T" + tid + " local variables are " + transaction.getLocal());
//                    db.print();
                } else {
                    rollback(transaction, db);
//                    System.out.println("T" + tid + " local variables are " + transaction.getLocal());
//                    db.print();
                    return 0;
                }
                checkFinishStatue(transaction,lockManager);
                return 1;
            case "A":
                //local[x] = local[x] + d
                transaction.add(operand1, operand2);
//                System.out.println("T" + tid + " local variables are " + transaction.getLocal());
//                db.print();
                //after executing one command, remove it from commandLines
                commandLines.remove(1);
                transaction.setCommandLines(commandLines);
                transaction.setCurrentCommandId(currentCommandId + 1);
                checkFinishStatue(transaction,lockManager);
                return 1;
            case "M":
                //local[x] = local[x] * d
                transaction.mult(operand1, operand2);
//                System.out.println("T" + tid + " local variables are " + transaction.getLocal());
//                db.print();
                //after executing one command, remove it from commandLines
                commandLines.remove(1);
                transaction.setCommandLines(commandLines);
                transaction.setCurrentCommandId(currentCommandId + 1);
                checkFinishStatue(transaction,lockManager);
                return 1;
            case "C":
                //local[x] = local[y]
                transaction.copy(operand1, operand2);
//                System.out.println("T" + tid + " local variables are " + transaction.getLocal());
//                db.print();
                //after executing one command, remove it from commandLines
                commandLines.remove(1);
                transaction.setCommandLines(commandLines);
                transaction.setCurrentCommandId(currentCommandId + 1);
                checkFinishStatue(transaction,lockManager);
                return 1;
            case "O":
                //local[x] = local[x] + local[y]
                transaction.combine(operand1, operand2);
//                System.out.println("T" + tid + " local variables are " + transaction.getLocal());
//                db.print();
                //after executing one command, remove it from commandLines
                commandLines.remove(1);
                transaction.setCommandLines(commandLines);
                transaction.setCurrentCommandId(currentCommandId + 1);
                checkFinishStatue(transaction,lockManager);
                return 1;
            case "P":
                //print the current elements in the database (x, y are ignored)
                transaction.display();
//                System.out.println("T" + tid + " local variables are " + transaction.getLocal());
//                db.print();
                //after executing one command, remove it from commandLines
                commandLines.remove(1);
                transaction.setCommandLines(commandLines);
                transaction.setCurrentCommandId(currentCommandId + 1);
                checkFinishStatue(transaction,lockManager);
                return 1;
            default:
        }
        return 1;
    }

    private static void checkFinishStatue(Transaction transaction, LockManager lockManager){
        List<String> commandLines = transaction.getCommandLines();
        if(commandLines.size()<=1){
            //If the rest numbers of commands less than 1 (the first line), mark this transaction finished and release all locks
            transaction.setFinished(true);
            //Release all locks
            lockManager.releaseAll(transaction.getTid());
        }
    }

    private static boolean ifDeadlock(List<Transaction> transactions) {
        transactions.removeIf(transaction -> transaction.getCommandLines().size() == 1);
        if (!transactions.isEmpty()) {
            for (Transaction transaction : transactions) {
                //If any unfinished transaction is not blocked, means there is no deadlock so far
                if (!transaction.isBlocked() && !transaction.isFinished()) {
                    return false;
                }
            }
            System.out.println("Deadlock!");
            System.exit(1);
        }
        return true;
    }

    private static void rollback(Transaction transaction, Database database) {
        List<RollbackLog> rollbackLogs = transaction.getRollbackLogs();
        for (RollbackLog rollbackLog : rollbackLogs) {
            database.write(rollbackLog.getK(), rollbackLog.getOriginalValue());
        }
        System.out.println("T" + transaction.getTid() + " rolled back");
    }


}
