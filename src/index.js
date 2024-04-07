// src/index.js

const parseQuery = require("./queryParser");
const readCSV = require("./csvReader");

function performInnerJoin(data, joinData, joinCondition, fields, table) {
  const result = [];

  // Iterate through each row of the main table
  for (const mainRow of data) {
    // Find matching rows in the joined table based on the join condition
    const matchingRows = joinData.filter((joinRow) => {
      // Check if the join condition is satisfied
      const mainValue = mainRow[joinCondition.left.split(".")[1]];
      const joinValue = joinRow[joinCondition.right.split(".")[1]];
      return mainValue === joinValue;
    });

    // Combine columns from both tables into a single row
    matchingRows.forEach((matchingRow) => {
      const combinedRow = {};

      // Add columns from the main table
      fields.forEach((field) => {
        combinedRow[`${table}.${field}`] = mainRow[field];
      });

      // Add columns from the joined table
      Object.keys(matchingRow).forEach((key) => {
        combinedRow[`${joinData}.${key}`] = matchingRow[key];
      });

      // Add the combined row to the result set
      result.push(combinedRow);
    });
  }

  return result;
}

function performLeftJoin(data, joinData, joinCondition, fields, table) {
  const result = [];

  // Iterate through each row of the main table
  for (const mainRow of data) {
    // Find matching rows in the joined table based on the join condition
    const matchingRows = joinData.filter((joinRow) => {
      // Check if the join condition is satisfied
      const mainValue = mainRow[joinCondition.left.split(".")[1]];
      const joinValue = joinRow[joinCondition.right.split(".")[1]];
      return mainValue === joinValue;
    });

    // Combine columns from both tables into a single row
    if (matchingRows.length > 0) {
      matchingRows.forEach((matchingRow) => {
        const combinedRow = {};

        // Add columns from the main table
        fields.forEach((field) => {
          combinedRow[`${table}.${field}`] = mainRow[field];
        });

        // Add columns from the joined table
        Object.keys(matchingRow).forEach((key) => {
          combinedRow[`${joinData}.${key}`] = matchingRow[key];
        });

        // Add the combined row to the result set
        result.push(combinedRow);
      });
    } else {
      // If no match is found in the joined table, add null values for joined table columns
      const combinedRow = {};

      // Add columns from the main table
      fields.forEach((field) => {
        combinedRow[`${table}.${field}`] = mainRow[field];
      });

      // Add null values for joined table columns
      Object.keys(joinData[0]).forEach((key) => {
        combinedRow[`${joinData}.${key}`] = null;
      });

      // Add the combined row to the result set
      result.push(combinedRow);
    }
  }

  return result;
}

function performRightJoin(data, joinData, joinCondition, fields, table) {
  const result = [];

  // Iterate through each row of the joined table
  for (const joinRow of joinData) {
    // Find matching rows in the main table based on the join condition
    const matchingRows = data.filter((mainRow) => {
      // Check if the join condition is satisfied
      const mainValue = mainRow[joinCondition.left.split(".")[1]];
      const joinValue = joinRow[joinCondition.right.split(".")[1]];
      return mainValue === joinValue;
    });

    // Combine columns from both tables into a single row
    if (matchingRows.length > 0) {
      matchingRows.forEach((matchingRow) => {
        const combinedRow = {};

        // Add columns from the main table
        fields.forEach((field) => {
          combinedRow[`${table}.${field}`] = matchingRow[field];
        });

        // Add columns from the joined table
        Object.keys(joinRow).forEach((key) => {
          combinedRow[`${joinData}.${key}`] = joinRow[key];
        });

        // Add the combined row to the result set
        result.push(combinedRow);
      });
    } else {
      // If no match is found in the main table, add null values for main table columns
      const combinedRow = {};

      // Add null values for main table columns
      fields.forEach((field) => {
        combinedRow[`${table}.${field}`] = null;
      });

      // Add columns from the joined table
      Object.keys(joinRow).forEach((key) => {
        combinedRow[`${joinData}.${key}`] = joinRow[key];
      });

      // Add the combined row to the result set
      result.push(combinedRow);
    }
  }

  return result;
}

async function executeSELECTQuery(query) {
  // src/index.js at executeSELECTQuery

  // Now we will have joinTable, joinCondition in the parsed query
  const { fields, table, whereClauses, joinTable, joinCondition } =
    parseQuery(query);
  let data = await readCSV(`${table}.csv`);

  // Perform INNER JOIN if specified
  if (joinTable && joinCondition) {
    const joinData = await readCSV(`${joinTable}.csv`);
    switch (joinType.toUpperCase()) {
      case "INNER":
        data = performInnerJoin(data, joinData, joinCondition, fields, table);
        break;
      case "LEFT":
        data = performLeftJoin(data, joinData, joinCondition, fields, table);
        break;
      case "RIGHT":
        data = performRightJoin(data, joinData, joinCondition, fields, table);
        break;
      // Handle default case or unsupported JOIN types
    }
  }

  // Apply WHERE clause filtering after JOIN (or on the original data if no join)
  const filteredData =
    whereClauses.length > 0
      ? data.filter((row) =>
          whereClauses.every((clause) => evaluateCondition(row, clause))
        )
      : data;

  // Select the specified fields
  return filteredData.map((row) => {
    const selectedRow = {};
    fields.forEach((field) => {
      selectedRow[field] = row[field];
    });
    return selectedRow;
  });
}
function evaluateCondition(row, clause) {
  const { field, operator, value } = clause;
  switch (operator) {
    case "=":
      return row[field] === value;
    case "!=":
      return row[field] !== value;
    case ">":
      return row[field] > value;
    case "<":
      return row[field] < value;
    case ">=":
      return row[field] >= value;
    case "<=":
      return row[field] <= value;
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}

module.exports = executeSELECTQuery;
