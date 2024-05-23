import { MongoClient } from 'mongodb';
import inquirer from 'inquirer';

async function fetchEmployeesByManager(managerEmpNo) {
    const client = new MongoClient('mongodb://localhost:27017', { useNewUrlParser: true, useUnifiedTopology: true });
    try {
        await client.connect();
        const db = client.db('company');
        const collection = db.collection('employees');

        const employees = await collection.find({
            'departments.is_manager': false, // Filter out managers
            'departments.manager_id': managerEmpNo // Filter by manager's ID
        }).toArray();

        console.log(employees);
        return employees;
    } finally {
        await client.close();
    }
}


async function fetchEmployeesByTitle(title) {
    const client = new MongoClient('mongodb://localhost:27017', { useNewUrlParser: true, useUnifiedTopology: true });
    try {
        await client.connect();
        const db = client.db('company');
        const collection = db.collection('employees');

        const employees = await collection.find({
            'titles.title': title
        }).toArray();

        console.log(employees);
        return employees;
    } finally {
        await client.close();
    }
}

async function fetchEmployeesByDepartment(departmentName) {
    const client = new MongoClient('mongodb://localhost:27017', { useNewUrlParser: true, useUnifiedTopology: true });
    try {
        await client.connect();
        const db = client.db('company');
        const collection = db.collection('employees');

        const deptNo = departmentNameToDeptNo[departmentName];

        const employees = await collection.find({
            'departments.dept_no': deptNo,
            'departments.is_manager': false // Filter out managers
        }).toArray();

        console.log(employees);
        return employees;
    } finally {
        await client.close();
    }
}


async function fetchAverageSalaryByDepartment() {
    const client = new MongoClient('mongodb://localhost:27017', { useNewUrlParser: true, useUnifiedTopology: true });
    try {
        await client.connect();
        const db = client.db('company');
        const collection = db.collection('employees');

        const averageSalaries = await collection.aggregate([
            { $unwind: "$departments" },
            { $unwind: "$salaries" },
            { $group: {
                _id: "$departments.dept_no",
                avgSalary: { $avg: "$salaries.salary" }
            }},
            { $sort: { _id: 1 } }
        ]).toArray();

        const formattedSalaries = averageSalaries.map(salary => ({
            department: salary._id,
            averageSalary: salary.avgSalary.toFixed(2)
        }));

        console.log("Average Salary by Department:");
        formattedSalaries.forEach(salary => {
            console.log(`Department: ${salary.department}, Average Salary: $${salary.averageSalary}`);
        });

        return formattedSalaries;
    } finally {
        await client.close();
    }
}


const departmentNameToDeptNo = {
    'Marketing': 'd001',
    'Finance': 'd002',
    'Human Resources': 'd003',
    'Production': 'd004',
    'Development': 'd005',
    'Quality Management': 'd006',
    'Sales': 'd007',
    'Research': 'd008',
    'Customer Service': 'd009'
};

async function main() {
    const { queryType } = await inquirer.prompt([
        {
            type: 'list',
            name: 'queryType',
            message: 'Choose the query type:',
            choices: ['By Manager', 'By Title', 'By Department', 'Average Salary by Department']
        }
    ]);

    switch (queryType) {
        case 'By Manager':
            const { managerEmpNo } = await inquirer.prompt([
                { type: 'input', name: 'managerEmpNo', message: 'Enter Manager Employee Number:' }
            ]);
            await fetchEmployeesByManager(Number(managerEmpNo));
            break;
        case 'By Title':
            const { title } = await inquirer.prompt([
                { type: 'input', name: 'title', message: 'Enter Title:' }
            ]);
            await fetchEmployeesByTitle(title);
            break;
        case 'By Department':
            const { departmentName } = await inquirer.prompt([
                { type: 'input', name: 'departmentName', message: 'Enter Department Name:' }
            ]);
            await fetchEmployeesByDepartment(departmentName);
            break;
        case 'Average Salary by Department':
            await fetchAverageSalaryByDepartment();
            break;
    }
}

main().catch(console.error);
