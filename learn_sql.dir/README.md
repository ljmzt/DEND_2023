# Learning SQL

### Purpose
This folder has information about some basic SQL (postgres) usages and tests to make sure I understand the database operations.

### Starting a server
Some of the steps listed below follow [gwangjinkim's github](https://gist.github.com/gwangjinkim/f13bf596fefa7db7d31c22efd1627c7a) and the official documents on [postgres's website](https://www.postgresql.org/docs/15/admin.html).
- I created a conda environment psql just for running the postgres server.
- To initialize the cluster, use
  ```
    initdb -D postgres_cluster.dir
  ```
   
  Note that although the command is called initdb,  it actually means starting a cluster, see [here](https://www.postgresql.org/docs/current/app-initdb.html). 
  The cluster will have you as a superuser with username the same as your system username. It also makes a folder and put some initial conf files in it. I kept the files unchanged and just used the default settings.
- To start the server, use
  ```
    pg_ctl -D postgres_cluster.dir -l logfile start
  ```
  Now you should see the postgres server is running by
  ```
    ps aux | grep postgres
  ```
- Create an user and database
  ```
    createuser -P student
    createdb --owner=student student_db
  ```
  The createuser command will ask for a password. For demonstration, I am using "student" for now. To actually make it authenticate the password if you are connecting from local, you need to change the method from "trust" to "md5" in the file pg_hba.conf. Please see [here](https://stackoverflow.com/questions/38954123/postgresqls-psql-exe-doesnt-validate-password) for details. 
  NOTE: if you want to change the authentication method, please make absolutely sure you have updated your superuser password first, as that was set to some default value which nobody knows. If that happens, please see [this](https://chartio.com/resources/tutorials/how-to-set-the-default-user-password-in-postgresql/), and it's an infinite pain to fix.
- Test connection  
  Now you should be able to use psql to connect to the database by using:
  ```
    psql -h localhost -U student -d student_db
  ```
  or
  ```
    psql postgresql://student:student@localhost/student_db
  ```
  It's interesting to note that, if you login as student, the prompt looks like this
  ```
    student_db=>
  ```
  but for superuser, it's:
  ```
    student_db=#
  ```
- To shut it down, use
  ```
    pg_ctl stop -D postgres_cluster.dir
  ```
- Some useful commands once you get inside psql are:
-- \q: quit  
-- \l: list of databases  
-- \du: list of roles (i.e. users in older terminology)  
-- \d or \dt: list relations (i.e. tables)  
-- \d table name: list columns of this table  
-- \dn: list of schemas  
-- \dtvs public.*: list everything (table, view and sequences) in public schema; the public schema is the default schema that all the created tables, views etc will go to.

It's actually funny that there's no file called student_db.db in the system, it got broken down to whole bunch of strange files, see [this](https://stackoverflow.com/questions/5052907/location-of-postgresql-database-on-os-x).

### Test 1: Compare Copy vs Insert (test_insert_vs_copy.py)
It is fairly obvious that one needs to use COPY instead of INSERT when dealing with large data set. This file shows that copying a n=1e+5 txt file that has only two columns (id and val) into a table takes ~0.1s when using COPY and ~13s when using INSERT.

NOTE: to make the COPY works, one needs to grant the user (student) privilage to read server side file:
```
GRANT pg_read_server_files TO student;
```

### Test 2: Join with and without primary key (test_primary_key.py)
I am curious how much faster it can be to do a join with and without primary key. The idea is that, say I use where table1.id = table2.id, if table2.id is the primary key, it won't do a whole table scan, but can directly find the table2.id.

To test this, I generated an n=1e+7 table. Inserting into table without primary key takes about 12 sec. Inserting into table with primary takes about 60 sec. Then I generated a shorter table with n=1e+4. Join into this table without primary key takes about 3 sec and takes < 0.1 sec with primary key, which is **REALLY** fast.

### Test 3: Transaction (test_transaction.py)
Transaction is an interesting property in database (see [this](https://www.postgresql.org/docs/16/tutorial-transactions.html) for reference).

Suppose we have this table accounts with a constraint on balance:
```
CREATE TABLE accounts (
  name text, 
  balance real CHECK (balance >= 0)
  )
```

and want to commit this transaction which transfer money from Alice's account to Bobs:
```
UPDATE accounts SET balance = balance + 50 WHERE name = 'Bob'
UPDATE accounts SET balance = balance - 50 WHERE name = 'Alice'
```

Say Alice only has 80 dollars in her account, only 1 transaction can be made. If I want to make 2 transactions, the second one will fail and won't transfer money to Bob's account even the update command on Bob's account occurs before the update on Alice's. 

I use asyncio to simulate two transactions, one happens slightly after another. For asyncio, please see [this](https://realpython.com/async-io-python/). Strictly speaking, it's not modeling concurrent transactions, but instead modeling a sequence of two transactions. But for testing, I think it's enough. 

The code shows that the later transaction indeed fails and rolls back to the beginning without adding money to Bob's account.