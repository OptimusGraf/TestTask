using Microsoft.Data.Sqlite;

public class DataBaseSqlite : IDataBase
{
    string connectionString = "Data Source=database.db;";

    public DataBaseSqlite()
    {

    }

    public void CreateTable()
    {

        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        using SqliteCommand commnand = connection.CreateCommand();
        commnand.CommandText = @"CREATE TABLE IF NOT EXISTS  Employees (
                                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        Name TEXT not NULL,
                                        SEX TEXT NOT NULL, 
                                        BIRTHDATE TEXT NOT NULL);";
        commnand.ExecuteNonQuery();
    }

    public void InsertEmployee(Employee employee)
    {
        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = @"INSERT INTO Employees (NAME,SEX,BIRTHDATE) 
                                    VALUES ($name,$sex,$dateOfBirth);";
        command.Parameters.AddWithValue("$name", employee.Name);
        command.Parameters.AddWithValue("$sex", employee.Sex.ToString());
        command.Parameters.AddWithValue("$dateOfBirth", employee.BirthDate.ToString("yyyy-MM-dd"));
        command.ExecuteNonQuery();

    }
    public IEnumerable<Employee> GetAllEmployees() 
    {
        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = @"SELECT  NAME,BIRTHDATE, SEX FROM Employees GROUP BY NAME,BIRTHDATE ORDER BY NAME,BIRTHDATE;";

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            yield return (new Employee(reader.GetString(0), reader.GetString(1), reader.GetString(2)));

        }

    }

    public void InsertManyEmployees(IEnumerable<Employee> employees, int Batch=10000)
    {

        using var connection = new SqliteConnection(connectionString);
        connection.Open();

        var transaction = connection.BeginTransaction();

        SqliteCommand command;
        SqliteParameter nameParam, sexParam, dateOfBirthParam; 
        CreateCommand(connection, out command, out nameParam, out sexParam, out dateOfBirthParam);
        command.Transaction = transaction;

        try
        { 
            int counter = 0;
            foreach (var employee in employees)
            {
                nameParam.Value = employee.Name;
                sexParam.Value = employee.Sex == Sex.Male ? "Male" : "Female";
                dateOfBirthParam.Value = employee.BirthDate.ToString("yyyy-MM-dd");
                command.ExecuteNonQuery();
                counter++;
                if (counter % Batch == 0)
                {
                    transaction.Commit();
                    transaction.Dispose();
                    transaction = connection.BeginTransaction();
                    command.Transaction= transaction; ;
                   
                }
            }
            transaction.Commit();

        }
        finally
        {
            transaction?.Dispose();
            command?.Dispose();

        }

        static void CreateCommand(SqliteConnection connection, out SqliteCommand command, out SqliteParameter nameParam, out SqliteParameter sexParam, out SqliteParameter dateOfBirthParam)
        {
           
            command = connection.CreateCommand();
            command.CommandText = @"INSERT INTO Employees (NAME, SEX,  BIRTHDATE) 
                                    VALUES ($name,$sex,$dateOfBirth);";
            nameParam = command.CreateParameter();
            nameParam.ParameterName = "$name"; command.Parameters.Add(nameParam);
            sexParam = command.CreateParameter();
            sexParam.ParameterName = "$sex"; command.Parameters.Add(sexParam);
            dateOfBirthParam = command.CreateParameter();
            dateOfBirthParam.ParameterName = "$dateOfBirth"; command.Parameters.Add(dateOfBirthParam);
            
        }
    }
    public IEnumerable<Employee> GetMaleSurnameF()
    {
        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT  Name,BIRTHDATE,Sex FROM Employees WHERE NAME LIKE 'F%' AND SEX ='Male'";
      
        var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var employee = new Employee(reader.GetString(0), reader.GetString(1), reader.GetString(2));
            yield return employee;

        }
    
       
    }
    public IEnumerable<Employee> GetMaleSurnameFOptimazed()
    {
        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT  Name,BIRTHDATE,Sex FROM Employees WHERE FirstLetter= 'F' AND SEX ='Male'";

        var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var employee = new Employee(reader.GetString(0), reader.GetString(1), reader.GetString(2));
            yield return employee;

        }


    }

    public void OptimazeForQuery5()
    {
        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = @"Alter Table Employees add column FirstLetter Text ;
                        UPDATE Employees
                        SET FirstLetter = SUBSTR(Name, 1, 1);";
                
       command.ExecuteNonQuery();

       CreateIndex();
    }
    private void CreateIndex()
    {
        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = @"CREATE INDEX IF NOT EXISTS idx_name_sex on Employees (FirstLetter,SEX);";
        command.ExecuteNonQuery();
    }
    public void UnOptimazeForQuery5()
    {
        DropIndex();
        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = @"ALTER TABLE Employees DROP COLUMN FirstLetter;";
        command.ExecuteNonQuery();
    }
    private void DropIndex()
    {
        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = @"DROP INDEX IF EXISTS idx_name_sex;";
        command.ExecuteNonQuery();
    }


}
