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
