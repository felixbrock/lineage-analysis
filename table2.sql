create table table2 as select department_name, last_name, first_name
    from employee_table inner join department_table
        on employee_table.department_id = department_table.department_id
    order by department_name, last_name, first_name;