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
-- \d: list relations (i.e. tables)
-- \d table name: list columns of this table

It's actually funny that there's no file called student_db.db in the system, it got broken down to whole bunch of strange files, see [this](https://stackoverflow.com/questions/5052907/location-of-postgresql-database-on-os-x)

### Connect to the server from other applications
