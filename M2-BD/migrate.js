import { Sequelize, DataTypes } from 'sequelize';
import { MongoClient } from 'mongodb';

const sequelize = new Sequelize('employees', 'root', 'root', {
    host: 'localhost',
    dialect: 'mysql',
    logging: false
});

const Employee = sequelize.define('employee', {
    emp_no: { type: DataTypes.INTEGER, primaryKey: true },
    birth_date: DataTypes.DATE,
    first_name: DataTypes.STRING,
    last_name: DataTypes.STRING,
    gender: DataTypes.ENUM('M', 'F'),
    hire_date: DataTypes.DATE
}, { timestamps: false, tableName: 'employees' });

const Salary = sequelize.define('salary', {
    emp_no: { type: DataTypes.INTEGER, primaryKey: true },
    salary: DataTypes.INTEGER,
    from_date: DataTypes.DATE,
    to_date: DataTypes.DATE
}, { timestamps: false, tableName: 'salaries' });

const Title = sequelize.define('title', {
    emp_no: { type: DataTypes.INTEGER, primaryKey: true },
    title: DataTypes.STRING,
    from_date: DataTypes.DATE,
    to_date: DataTypes.DATE
}, { timestamps: false, tableName: 'titles' });

const DeptEmp = sequelize.define('deptEmp', {
    emp_no: { type: DataTypes.INTEGER, primaryKey: true },
    dept_no: DataTypes.STRING,
    from_date: DataTypes.DATE,
    to_date: DataTypes.DATE
}, { timestamps: false, tableName: 'dept_emp' });

const DeptManager = sequelize.define('deptManager', {
    emp_no: { type: DataTypes.INTEGER, primaryKey: true },
    dept_no: DataTypes.STRING,
    from_date: DataTypes.DATE,
    to_date: DataTypes.DATE
}, { timestamps: false, tableName: 'dept_manager' });

async function createIndexes() {
    try {
        const existingIndexesSalaries = await sequelize.query("SHOW INDEX FROM salaries WHERE Key_name = 'idx_emp_no'");
        if (existingIndexesSalaries[0].length === 0) {
            await sequelize.query('CREATE INDEX idx_emp_no ON salaries(emp_no)');
            console.log('Index idx_emp_no created on salaries');
        } else {
            console.log('Index idx_emp_no already exists on salaries');
        }

        const existingIndexesTitles = await sequelize.query("SHOW INDEX FROM titles WHERE Key_name = 'idx_emp_no'");
        if (existingIndexesTitles[0].length === 0) {
            await sequelize.query('CREATE INDEX idx_emp_no ON titles(emp_no)');
            console.log('Index idx_emp_no created on titles');
        } else {
            console.log('Index idx_emp_no already exists on titles');
        }

        const existingIndexesTitlesTitle = await sequelize.query("SHOW INDEX FROM titles WHERE Key_name = 'idx_title'");
        if (existingIndexesTitlesTitle[0].length === 0) {
            await sequelize.query('CREATE INDEX idx_title ON titles(title)');
            console.log('Index idx_title created on titles');
        } else {
            console.log('Index idx_title already exists on titles');
        }

        const existingIndexesDeptEmp = await sequelize.query("SHOW INDEX FROM dept_emp WHERE Key_name = 'idx_emp_no'");
        if (existingIndexesDeptEmp[0].length === 0) {
            await sequelize.query('CREATE INDEX idx_emp_no ON dept_emp(emp_no)');
            console.log('Index idx_emp_no created on dept_emp');
        } else {
            console.log('Index idx_emp_no already exists on dept_emp');
        }

        const existingIndexesDeptEmpDeptNo = await sequelize.query("SHOW INDEX FROM dept_emp WHERE Key_name = 'idx_dept_no'");
        if (existingIndexesDeptEmpDeptNo[0].length === 0) {
            await sequelize.query('CREATE INDEX idx_dept_no ON dept_emp(dept_no)');
            console.log('Index idx_dept_no created on dept_emp');
        } else {
            console.log('Index idx_dept_no already exists on dept_emp');
        }

        const existingIndexesDeptManager = await sequelize.query("SHOW INDEX FROM dept_manager WHERE Key_name = 'idx_emp_no'");
        if (existingIndexesDeptManager[0].length === 0) {
            await sequelize.query('CREATE INDEX idx_emp_no ON dept_manager(emp_no)');
            console.log('Index idx_emp_no created on dept_manager');
        } else {
            console.log('Index idx_emp_no already exists on dept_manager');
        }
    } catch (error) {
        console.error('Error creating index:', error);
    }
}

async function fetchEmployeesBatch(offset, limit) {
    return await Employee.findAll({ offset, limit });
}

async function fetchRelatedData(model, empNo) {
    return await model.findAll({ where: { emp_no: empNo } });
}

async function migrateData() {
    await sequelize.authenticate();
    console.log('Connection to MySQL has been established successfully.');

    await createIndexes();
    console.log('Indexes have been created successfully in MySQL.');

    const client = new MongoClient('mongodb://localhost:27017', { useNewUrlParser: true, useUnifiedTopology: true });
    await client.connect();
    const db = client.db('company');
    const collection = db.collection('employees');

    // Creating indexes in MongoDB
    await collection.createIndex({ emp_no: 1 });
    console.log('Indexes have been created successfully in MongoDB.');

    const batchSize = 5000;
    let offset = 0;
    let employeesBatch;

    // Map to store department managers
    const departmentManagers = {};

    do {
        employeesBatch = await fetchEmployeesBatch(offset, batchSize);
        offset += batchSize;

        const bulkOperations = [];
        for (const emp of employeesBatch) {
            const salaries = await fetchRelatedData(Salary, emp.emp_no);
            const titles = await fetchRelatedData(Title, emp.emp_no);
            const deptEmp = await fetchRelatedData(DeptEmp, emp.emp_no);
            const deptManager = await fetchRelatedData(DeptManager, emp.emp_no);

            const departments = [];
            for (const dept of deptEmp) {
                const isManager = deptManager.some(manager => manager.dept_no === dept.dept_no);
                departments.push({
                    dept_no: dept.dept_no,
                    from_date: dept.from_date,
                    to_date: dept.to_date,
                    is_manager: isManager
                });

                // If the employee is a manager, store their ID for the department
                if (isManager) {
                    departmentManagers[dept.dept_no] = emp.emp_no;
                }
            }

            const transformedEmployee = {
                ...emp.toJSON(),
                salaries: salaries.map(sal => sal.toJSON()),
                titles: titles.map(tit => tit.toJSON()),
                departments
            };

            bulkOperations.push({
                updateOne: {
                    filter: { emp_no: emp.emp_no },
                    update: { $set: transformedEmployee },
                    upsert: true
                }
            });
        }

        if (bulkOperations.length > 0) {
            await collection.bulkWrite(bulkOperations);
        }

        console.log(`Processed ${offset} / 300000+ employees`);
    } while (employeesBatch.length > 0);

    // Add department managers to each department
    const departmentsBulkOperations = Object.keys(departmentManagers).map(dept_no => ({
        updateMany: {
            filter: { "departments.dept_no": dept_no },
            update: { $set: { "departments.$.manager_id": departmentManagers[dept_no] } }
        }
    }));

    if (departmentsBulkOperations.length > 0) {
        await collection.bulkWrite(departmentsBulkOperations);
    }

    await sequelize.close();
    await client.close();
    console.log('Data migration completed successfully');
}

migrateData().catch(console.error);
