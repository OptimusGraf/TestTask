using System.Globalization;

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
