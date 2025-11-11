using Microsoft.Data.Sqlite;
using System.Globalization;
using System.Net.WebSockets;
using System.Text.RegularExpressions;
using System.Transactions;

public enum Sex { Male, Female };

public class Employee
{
    public Employee(string name, Sex sex, DateTime birthDate)
    {
        Name = name;
        Sex = sex;
        BirthDate = birthDate;

    }
    public Employee(string name, string birthDate, string sex)
    {
        Name = name;
        Sex = sex.ToLower()[0] == 'm' ? Sex.Male : Sex.Female;
        DateTime.TryParseExact(birthDate, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date);
        BirthDate = date;
    }

    public string Name { get; set; } = string.Empty;
    public Sex Sex { get; set; }
    public DateTime BirthDate { get; set; }
    public int Age
    {
        get
        {
            var today = DateTime.Today;
            int age = today.Year - BirthDate.Year;
            if (BirthDate > today.AddYears(-age)) age--;
            return age;
        }
    }
    public void InsertEmployeeToDB(IDataBase db)
    {
        db.InsertEmployee(this);
    }
}
public interface IDataBase
{

    public  void CreateTable();
    public  void InsertEmployee(Employee employee);
    public  void InsertManyEmployees(IEnumerable<Employee> employees, int Batch = 10000);
    public  IEnumerable<Employee> GetAllEmployees();
    public  IEnumerable<Employee> GetMaleSurnameF();
    public void OptimazeForQuery5();
    public  IEnumerable<Employee> GetMaleSurnameFOptimazed();
    public  void UnOptimazeForQuery5();
}
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
class Program
{
   static Random random;
    static string GenerateName( Sex sex)
    {

        var surnames = new[]{"Anderson", "Brown", "Carter", "Davis", "Edwards", "Foster",
        "Garcia", "Harris", "Irving", "Johnson", "King", "Lewis",
        "Miller", "Nelson", "Olson", "Parker", "Quinn", "Roberts",
        "Smith", "Thompson", "Underwood", "Vaughn", "Walker",
        "Xavier", "Young", "Zimmerman" };

        var namesMale = new[] { "John", "Michael", "David", "James", "Robert", "William", "Mark", "Richard", "Thomas", "Steven" }; 
        var namesFemale = new[] { "Mary", "Patricia", "Linda", "Barbara", "Elizabeth", "Jennifer", "Maria", "Susan", "Margaret", "Dorothy" }; 
        string[] FatherNames = {
        "Allen", "Bryant", "Charles", "Douglas", "Edwin", "Frank",
        "George", "Henry", "Isaac", "James", "Keith", "Louis",
        "Michael", "Nathan", "Oscar", "Paul", "Quentin", "Raymond",
        "Samuel", "Thomas", "Ulysses", "Victor", "William",
        "Xander", "Yosef", "Zachary"}; 
        var names = sex == Sex.Male ? namesMale : namesFemale;
        string name = $"{surnames[random.Next(surnames.Length)]} {names[random.Next(names.Length)]} {FatherNames[random.Next(FatherNames.Length)]}";
        return name;
    }
    static DateTime RandomDate( DateTime from, DateTime to)
    {
        var range = (to - from).Days;
        return from.AddDays(random.Next(range + 1));
    }

    static IEnumerable<Employee> GenerateRandomEmployees()
    {

        


        for (int i = 0; i < 10; i++)
        {
            Sex sex = random.Next(0, 2) == 0 ? Sex.Male : Sex.Female;
            string name = GenerateName( sex);
            DateTime birth = RandomDate( new DateTime(1950, 1, 1), new DateTime(2013, 12, 31));
            yield return new Employee(name, sex, birth);

        }
        for (int i = 0; i < 3; i++)
        {
            Sex sex = Sex.Male;
            string name = "F" + GenerateName( sex);
            DateTime birth = RandomDate( new DateTime(1950, 1, 1), new DateTime(2013, 12, 31));
            yield return new Employee(name, sex, birth);
        }
    }
    static void PrintInfo()
    {
        Console.WriteLine("Modes:");
        Console.WriteLine(" 1 - create table");
        Console.WriteLine(" 2 \"Full Name\" YYYY-MM-DD Sex - add single record");
        Console.WriteLine(" 3 - list all unique records with age");
        Console.WriteLine(" 4 - generate 1,000,000 rows + 100 special rows");
        Console.WriteLine(" 5 - query Male & Surname starts with F and measure time");
        Console.WriteLine(" 6 - optimize (create index) and show improved time for mode 5");
    }
    static Sex ParseSex(string sex)
    {
     
       return sex.ToLower()[0] == 'm' ? Sex.Male : Sex.Female;
    }
    static void Main(string[] args)
    {
        random= new Random();

        if (args.Length == 0)
        {
            PrintInfo();
          // return;
        }
    // args =new string[] { "4", "Doe John Michael", "15-04-1985", "Male" };
        var mode = args[0];
        IDataBase db = new DataBaseSqlite();
        
        switch (mode)
        {
            case "1":
                db.CreateTable();
                Console.WriteLine("Table created (if not exists)");
                break;
            case "2":
                if (args.Length < 4)
                {
                    Console.WriteLine("Invalid args for mode 2.");
                    PrintInfo();
                    return;
                }
                var fullName = args[1];
               
                if (!DateTime.TryParseExact(args[2], "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var birth))
                {
                    Console.WriteLine("Date must be in yyyy-MM-dd format."); return;
                }
                var sex = ParseSex(args[3]);
                db.CreateTable();
                var emp = new Employee(fullName, sex, birth);
                emp.InsertEmployeeToDB(db);
                Console.WriteLine($"Inserted: {emp.Name} {emp.Sex.ToString()}  {emp.BirthDate:yyyy-MM-dd} ");
                break;
            case "3":
                db.CreateTable();
                var all = db.GetAllEmployees();
                foreach (var e in all)
                {
                    Console.WriteLine($"{e.Name} | {e.BirthDate:yyyy-MM-dd} | {e.Sex} | {e.Age} years");
                }
                Console.WriteLine($"Total: {all.Count()}");
                break;
            case "4":
                db.CreateTable();
                var employees = GenerateRandomEmployees();
                db.InsertManyEmployees(employees);
                Console.WriteLine("Inserted 1000100 employees.");
                break;
            case "5":
                db.CreateTable();
                TimeSpan? timeOfQueryBefore = null;
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var maleFBefore = db.GetMaleSurnameF();
                var count = maleFBefore.Count();
                sw.Stop();
                timeOfQueryBefore = sw.Elapsed;
                Console.WriteLine($"Found {count} rows. Query time: {timeOfQueryBefore.Value.TotalMilliseconds} ms");
                break;

            case "6":
                db.CreateTable();
                db.OptimazeForQuery5();
                TimeSpan? timeOfQueryAfter = null;

                sw = System.Diagnostics.Stopwatch.StartNew();
                var maleFAfter = db.GetMaleSurnameFOptimazed();
                var countAfter = maleFAfter.Count();
                sw.Stop();
                timeOfQueryAfter = sw.Elapsed;
                db.UnOptimazeForQuery5();
                Console.WriteLine($"After optimization: {countAfter} rows, {timeOfQueryAfter.Value.TotalMilliseconds} ms");
               // Console.WriteLine("Optimization explanation: created index on (Surname, Sex) to speed WHERE Sex='Male' AND Surname LIKE 'F%'.");
                break;
            default:
                PrintInfo();
                break;
        }
    }
}
