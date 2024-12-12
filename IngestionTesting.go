package main

//
//import (
//	"encoding/csv"
//	"fmt"
//	"github.com/newrelic/nri-mssql/src/args"
//	"github.com/newrelic/nri-mssql/src/queryAnalysis/connection"
//	"log"
//	"os"
//	"strconv"
//)
//
//func main() {
//	argsList := args.ArgumentList{
//		Username:               "sa",
//		Password:               "password",
//		Hostname:               "20.235.136.68",
//		Port:                   "1433",
//		Timeout:                "30",
//		EnableSSL:              true,
//		TrustServerCertificate: true,
//	}
//
//	sqlConnection, err := connection.NewConnection(&argsList)
//	if err != nil {
//		log.Println("Error creating connection to SQL Server", err)
//		return
//	}
//
//	queries := []string{
//		"SELECT * FROM Person.Person;",
//		"SELECT COUNT(*) FROM Sales.SalesOrderHeader;",
//		"SELECT * FROM Production.Product;",
//		"SELECT COUNT(*) FROM HumanResources.Employee;",
//		"SELECT FirstName, LastName FROM Person.Person;",
//		"SELECT AVG(ListPrice) FROM Production.Product;",
//		"SELECT MAX(OrderQty) FROM Sales.SalesOrderDetail;",
//		"SELECT * FROM Production.ProductModel;",
//		"SELECT DISTINCT Name FROM Production.ProductCategory;",
//		"SELECT COUNT(DISTINCT TerritoryID) FROM Sales.SalesTerritory;",
//		"SELECT AddressLine1, City, StateProvinceID FROM Person.Address;",
//		"SELECT Name FROM HumanResources.Department WHERE GroupName = 'Research and Development';",
//		"SELECT ProductID, Name FROM Production.Product WHERE ListPrice > 1000;",
//		"SELECT SalesOrderID, OrderDate FROM Sales.SalesOrderHeader WHERE OrderDate > '2022-01-01';",
//		"SELECT BusinessEntityID, LoginID FROM HumanResources.Employee WHERE JobTitle = 'Engineering Manager';",
//		"SELECT * FROM Sales.SalesPerson;",
//		"SELECT CountryRegionCode, Name FROM Person.CountryRegion WHERE Name LIKE 'C%';",
//		"SELECT EmailAddress FROM Person.EmailAddress WHERE BusinessEntityID = 1;",
//		"SELECT PhoneNumber FROM Person.PersonPhone WHERE PhoneNumberTypeID = 1;",
//		"SELECT DepartmentID, Name FROM HumanResources.Department ORDER BY Name;",
//		"SELECT BusinessEntityID, NationalIDNumber FROM HumanResources.Employee ORDER BY HireDate DESC;",
//		"SELECT JobTitle, COUNT(*) FROM HumanResources.Employee GROUP BY JobTitle;",
//		"SELECT StateProvinceID, Name FROM Person.StateProvince WHERE CountryRegionCode = 'US';",
//		"SELECT Name, DaysToManufacture FROM Production.Product WHERE DaysToManufacture < 4;",
//		"SELECT ProductNumber FROM Production.Product WHERE MakeFlag = 1;",
//		"SELECT ContactTypeID, Name FROM Person.ContactType;",
//		"SELECT SalesOrderDetailID FROM Sales.SalesOrderDetail ORDER BY LineTotal DESC;",
//		"SELECT DocumentNode, Title FROM Production.Document;",
//		"SELECT COUNT(*) FROM Sales.Currency;",
//		"SELECT BusinessEntityID, ContactTypeID FROM Person.BusinessEntityContact;",
//		"SELECT TerritoryID, Name, CountryRegionCode FROM Sales.SalesTerritory;",
//		"SELECT SpecialOfferID, Description FROM Sales.SpecialOffer WHERE DiscountPct = 0.1;",
//		"SELECT * FROM Production.ProductReview WHERE Rating > 4;",
//		"SELECT ProductPhotoID FROM Production.ProductProductPhoto WHERE ProductID = 747;",
//		"SELECT CreditCardID, CardNumber FROM Sales.CreditCard;",
//		"SELECT Name, ListPrice FROM Production.Product WHERE ListPrice BETWEEN 100 AND 200;",
//		"SELECT * FROM Sales.SalesReason WHERE Name LIKE '%Marketing%';",
//		"SELECT SalesOrderDetailID, LineTotal FROM Sales.SalesOrderDetail WHERE ProductID = 750;",
//		"SELECT BusinessEntityID, Name FROM Sales.Store WHERE Demographics IS NOT NULL;",
//		"SELECT ReviewDate FROM Production.ProductReview ORDER BY ReviewDate DESC;",
//		"SELECT * FROM Person.AddressType;",
//		"SELECT ShiftID, StartTime, EndTime FROM HumanResources.Shift;",
//		"SELECT * FROM Sales.SalesOrderHeader WHERE TotalDue > 5000;",
//		"SELECT * FROM HumanResources.EmployeeDepartmentHistory WHERE EndDate IS NULL;",
//		"SELECT * FROM Production.ProductCostHistory WHERE ProductID BETWEEN 700 AND 800;",
//		"SELECT * FROM Purchasing.ProductVendor WHERE MinOrderQty > 10;",
//		"SELECT * FROM Person.StateProvince WHERE StateProvinceCode IN ('WA', 'OR');",
//		"SELECT * FROM Purchasing.PurchaseOrderHeader WHERE Freight > 70;",
//		"SELECT * FROM HumanResources.JobCandidate WHERE Resume IS NOT NULL;",
//		"SELECT * FROM Person.Address WHERE AddressLine1 LIKE '%Street%';",
//	}
//
//	successCountMap := make([]int, len(queries))
//
//	// Execute each query and track its execution statistics
//	for i, query := range queries {
//		prefix := "USE AdventureWorks2016;"
//		rows, err := sqlConnection.Connection.Queryx(prefix + query)
//		if err != nil {
//			log.Printf("Error executing query [%d]: %s - %v\n", i, query, err)
//		} else {
//			successCountMap[i]++
//			log.Println(rows) // you can process the rows here if needed
//		}
//	}
//
//	// Read existing CSV file
//	fileName := "query_statistics.csv"
//	var csvData [][]string
//
//	// Check if the CSV file already exists
//	if _, err := os.Stat(fileName); err == nil {
//		file, err := os.Open(fileName)
//		if err != nil {
//			log.Fatal("Error reading CSV file", err)
//		}
//		defer file.Close()
//
//		reader := csv.NewReader(file)
//		csvData, err = reader.ReadAll()
//		if err != nil {
//			log.Fatal("Error parsing CSV file", err)
//		}
//	}
//
//	// Update or Insert logic
//	indexMap := make(map[int]int)
//	for i, row := range csvData {
//		if len(row) < 4 || row[0] == "QueryText" {
//			continue
//		}
//		index, err := strconv.Atoi(row[2])
//		if err != nil {
//			continue
//		}
//		indexMap[index] = i // map index to the position in the csvData slice
//	}
//
//	for i, query := range queries {
//		executionCount := 1
//		if idx, exists := indexMap[i]; exists {
//			// Update existing row
//			execCnt, _ := strconv.Atoi(csvData[idx][1])
//			succCnt, _ := strconv.Atoi(csvData[idx][3])
//			csvData[idx][1] = strconv.Itoa(execCnt + executionCount)
//			csvData[idx][3] = strconv.Itoa(succCnt + successCountMap[i])
//		} else {
//			// Insert new row
//			record := []string{
//				query,
//				strconv.Itoa(executionCount),
//				strconv.Itoa(i),
//				strconv.Itoa(successCountMap[i]),
//			}
//			csvData = append(csvData, record)
//		}
//	}
//
//	// Write updated CSV data back to the file
//	file, err := os.Create(fileName)
//	if err != nil {
//		log.Fatal("Could not create CSV file", err)
//	}
//	defer file.Close()
//
//	writer := csv.NewWriter(file)
//	defer writer.Flush()
//
//	// Write the header if not present
//	if len(csvData) == 0 || csvData[0][0] != "QueryText" {
//		writer.Write([]string{"QueryText", "ExecutionCount", "IndexOfQuery", "SuccessCount"})
//	}
//	writer.WriteAll(csvData)
//
//	fmt.Println("Finished executing queries and updating CSV.")
//}
