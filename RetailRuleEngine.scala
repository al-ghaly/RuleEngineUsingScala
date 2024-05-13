package Project

import scala.io.{BufferedSource, Source}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.{LocalDate, LocalDateTime}
import math.ceil
import java.sql.{DriverManager, PreparedStatement, SQLException, SQLTimeoutException}
import java.text.SimpleDateFormat
import java.io.IOException
import java.io.FileNotFoundException
import java.lang.ClassNotFoundException
import java.io.{File, FileOutputStream, PrintWriter}



/**
 * The `RetailRuleEngine` object serves as the entry point for the retail discount application. It extends `App`,
 * enabling direct execution. The application reads transaction data from a CSV file, processes each transaction
 * through a set of business rules to calculate discounts, and writes the processed transactions to an Oracle
 * database.
 *
 * <p> This application is designed to automate the calculation of dynamic pricing adjustments based on various
 * factors such as product expiry dates, purchase quantities, special sale dates, and product categories.
 * It demonstrates the use of functional programming paradigms in Scala to manage and apply complex business
 * rules in a retail context.
 *
 * <p> Key functionalities include:
 * <ul>
 * <li>Reading transaction data from a CSV file.</li>
 * <li>Applying multiple discount rules to each transaction.</li>
 * <li>Normalizing combined discounts to ensure realistic pricing.</li>
 * <li>Batch inserting processed transactions into an Oracle database for persistence.</li>
 * </ul>
 *
 * @example To run this application, ensure that the CSV file path and database credentials are correctly
 *          configured, and then execute the program in an environment where Scala and a JVM are available.
 *          The application will automatically process the transactions file and update the database.
 * @note This application requires access to an Oracle database, and proper configuration of JDBC drivers.
 *       Exception handling is implemented to manage common errors such as file not found or database connection issues.
 * @see java.sql.PreparedStatement for details on how SQL operations are executed.
 * @see scala.io.Source for details on how file reading is handled.
 */
object RetailRuleEngine extends App {

    /**
     * Represents a transaction with all relevant details necessary for processing sales and applying discounts.
     *
     * @param timestamp     The date and time when the transaction was made, formatted as an ISO 8601 string.
     * @param productName   The name of the product involved in the transaction.
     * @param expiryDate    The expiry date of the product, useful for determining discounts based on product shelf life.
     * @param quantity      The quantity of the product sold in this transaction.
     * @param unitPrice     The unit price of the product at the time of the transaction.
     * @param channel       The sales channel through which the transaction was completed (e.g., online, store).
     * @param paymentMethod The payment method used for the transaction (e.g., Visa, cash).
     */
    private case class Transaction(timestamp: String, productName:String, expiryDate: String,
                                   quantity: Int, unitPrice: Double, channel:String, paymentMethod:String)

    /**
     * Represents a processed transaction that includes the original transaction details along with computed financials
     * such as discounts applied and the total due after discounts.
     *
     * @param originalTrx The original `Transaction` data before any processing.
     * @param discount    The discount percentage applied to the transaction.
     * @param totalDue    The final amount due after applying the discount to the transaction.
     */
    private case class ProcessedTransaction(originalTrx: Transaction, discount: Double, totalDue: Double)

    /**
     * Writes a single line to a log using a provided `PrintWriter`. This method encapsulates the basic functionality
     * of writing text data to logs, making it reusable and maintaining a clean and simple interface for logging
     * throughout the application.
     *
     * <p> This utility method is intended to abstract the direct use of `PrintWriter` and provide a centralized method
     * for writing log entries, which can be useful for implementing more complex logging behaviors in the future,
     * such as conditional logging or automated timestamping.
     *
     * @param writer The `PrintWriter` object used to write to the log. This writer must be properly initialized
     *               and opened before being passed to this method.
     * @param line   The string to be written to the log. This is the actual log message, which could include
     *               information about application state, errors, or other significant events.
     * @param level   The string to represent the log level of the line, Warning, Info, Error.
     * @example Usage:
     * {{{
     *          val writer = new PrintWriter(new FileWriter("app.log", true))
     *          writeLog(writer, "Starting application.")
     *          writeLog(writer, "Application error: unable to access database.")
     *          writer.close()
     *           }}}
     * @note It's important to manage the `PrintWriter` resource externally, ensuring it's opened before calling this
     *       method and closed appropriately when all writing is completed to avoid resource leaks.
     */
    private def writeLog(writer: PrintWriter, line: String, level: String): Unit = {
        val logFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val logTimestamp: String = LocalDateTime.now().format(logFormatter)
        writer.write(f"$logTimestamp  $level  $line \n")
    }

    /**
     * Converts a raw CSV line into a `Transaction` object. This function parses a comma-separated string
     * representing transaction data into the corresponding fields of the `Transaction` case class.
     *
     * <p> This function is critical for initial data ingestion, transforming string data from CSV files
     * into structured `Transaction` objects for further processing.
     *
     * @param line The string input from a CSV file, containing comma-separated transaction data.
     * @return A `Transaction` object with fields populated from the CSV data.
     * @example To convert a CSV line to a transaction object:
     * {{{
     *          val csvLine = "2023-04-18T18:18:40Z,Wine - White Pinot Grigio,2023-06-10,6,122.47,Store,Visa"
     *          val transaction = toTrx(csvLine)
     *           }}}
     * @see Transaction
     */
    private def toTrx(line: String): Transaction = {
        // Specify the line's arguments
        val args = line.split(",")
        // Assure that we have enough fields to initiate a transaction object
        if (args.length >= 7)
            // Return a Transaction Object
            Transaction(args(0), args(1), args(2), args(3).toInt, args(4).toDouble, args(5), args(6))
        // Return a dummy transaction representing an invalid line
        else  Transaction("-1", "", "", 0, 0.0, "", "")
    }

    /**
     * Reads transaction data from a specified file path and returns it as a list of strings. Each string represents
     * a line from the file, which corresponds to a single transaction. The function skips the first line assuming
     * it contains headers.
     *
     * <p> This function is essential for data ingestion, converting raw CSV file data into a manageable format for
     * further processing. It is designed to handle large files efficiently by reading all lines into memory at once,
     * which is suitable for files that are not excessively large.
     *
     * @param path The file path of the CSV containing transaction data.
     * @return A tuple of 2 elements
     *         list of strings, where each string represents a transaction line from the CSV file. <br>
     *         an integer representing the status code where <br>
     *              0 represents success and empty file <br>
     *              -1 represents FileNotFoundException <br>
     *              -2 represents IOException <br>
     *              Positive integer represents the number of lines <br>
     * @example To read transaction data from a file:
     * {{{
     *          val transactions = readData("src/main/resources/transactions.csv")
     *           }}}
     * @throws FileNotFoundException if the file does not exist at the specified path.
     * @throws IOException if an I/O error occurs opening the file.
     */
    private def readData(path:String): (List[String], Int) = {
        try {
            val source: BufferedSource = Source.fromFile(path)
            // Return the body of the file as a list of String
            val lines: List[String] = source.getLines().drop(1).toList
            // Close the file after reading
            source.close()
            // Return the read lines and the lines count
            (lines, lines.length)
        }
        // If the file does not exist
        catch
            // Return an empty list and an error code
            case e: IOException => (List.empty, -2)
            case e: FileNotFoundException => (List.empty, -1)
    }

    /**
     * Writes a list of processed transactions into an Oracle database. This function uses JDBC to connect to the database
     * and perform a batch insert, which is efficient for writing large numbers of transactions.
     *
     * <p> Each processed transaction contains details such as the timestamp, product name, expiry date, and calculated
     * financial figures which are inserted into the `SALES` table. The function also handles SQL exceptions and ensures
     * the database connection is closed after operations are completed.
     *
     * @param trxs A list of `ProcessedTransaction` objects to be inserted into the database.
     * @param writer A Print Writer object to be used to write log entries.
     * @return An integer representing the status code of the operation, while <br>
     *         * 1: represents a success code. <br>
     *         * -1: represents SQL Exception. <br>
     *         * -2: represents SQL TimeOut Exception. <br>
     *         * -3: Represents an error locating the JDBC Jar <br>
     *         * -4: Represents other errors. <br>
     * @example To write processed transaction data to a database:
     * {{{
     *          val processedTransactions = List(ProcessedTransaction(transaction, 0.05, 115.00))
     *          val status = writeData(processedTransactions)
     *           }}}
     * @throws SQException if a database access error occurs or the URL is null.
     * @throws ClassNotFoundException if the Oracle JDBC driver class is not found.
     */
    private def writeData(trxs: List[ProcessedTransaction], writer: PrintWriter):Int = {
        writeLog(writer, "Trying to connect to the database ...", "Info")
        // Specify the URL for the Database Server
        val url = "jdbc:oracle:thin:@localhost:1521:XE"
        // Specify the username, password to use to connect to the DB
        val username = "Analytical"
        val password = "123"
        // Specify the timestamp and expiry date format for the transaction entry
        val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        try {
            // Locate the JDBC Class => can throw a ClassNotFoundException
            Class.forName("oracle.jdbc.driver.OracleDriver")
            // Connect to the DB => Can throws a SQLTimeoutException or SQLException
            val connection = DriverManager.getConnection(url, username, password)
            writeLog(writer, "Successfully connected to the database", "Info")

            // SQL statement for inserting data
            val sql = """
                        INSERT INTO SALES (timestamp, product_name, expiry_date, quantity,
                                           unit_price, channel, payment_method, discount, total_due)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                      """

            // Prepare statement with the SQL
            val statement: PreparedStatement = connection.prepareStatement(sql)

            writeLog(writer, "Preparing the insert statements...", "Info")
            // Set parameters for the insert statement
            trxs.foreach(trx => try {
                    // Convert the timestamp string to a timestamp object
                    val timestamp = new java.sql.Timestamp(timestampFormat.parse(trx.originalTrx.timestamp).getTime)
                    // Fill the data for each attribute
                    statement.setTimestamp(1, timestamp)
                    statement.setString(2, trx.originalTrx.productName)
                    // Convert the expiry date to a Date Object
                    val expiryDate = new java.sql.Date(dateFormat.parse(trx.originalTrx.expiryDate).getTime)
                    statement.setDate(3, expiryDate)
                    statement.setInt(4, trx.originalTrx.quantity)
                    statement.setDouble(5, trx.originalTrx.unitPrice)
                    statement.setString(6, trx.originalTrx.channel)
                    statement.setString(7, trx.originalTrx.paymentMethod)
                    statement.setDouble(8, trx.discount)
                    statement.setDouble(9, trx.totalDue)
                    // Add the Insert Statement to the Execute batch
                    statement.addBatch()
                }
                catch {
                    case e: Exception => writeLog(writer, f"Insert statement for ${trx.originalTrx.timestamp} " +
                                                          f"transaction failed", "Warning")
                }
            )

            // Execute the insert statement
            statement.executeBatch()

            // Clean up environment
            statement.close()
            connection.close()
            1 // Return the success code
        } catch {
            case e: SQLException =>
                -1 // return SQL Exception Code
            case e: SQLTimeoutException =>
                -2 // Return SQLTimeOutException Code
            case e: ClassNotFoundException =>
                -3 // Return ClassNotFoundExceptionCode
            case e: Exception =>
                -4 // Return a general error code
        }
    }

    /**
     * Calculates a discount based on the remaining days before the product expires. If the product has 30 days or more
     * until expiration, it is not qualified for discount. If the product has fewer than 30 days, a discount
     * proportional to the days remaining is applied, up to a maximum of 30% as the expiry date approaches.
     *
     * <p> The method parses the `timestamp` and `expiryDate` from the `Transaction` object, calculates the number
     * of days between them, and applies a discount calculation based on the days remaining.
     *
     * <p> This calculation is critical for items that are perishable and need to be sold before expiring to avoid
     * losses, thus the function helps in dynamically pricing items close to their expiry date.
     *
     * @param trx The transaction for which the expiry day-based discount is to be calculated. This transaction
     *            contains the product's expiry date and the sale's timestamp among other details.
     * @param writer A Print Writer object to be used to write log entries.
     * @return The discount percentage based on expiry days. Returns 0 if more than 29 days remain, otherwise
     *         returns a value that increases as the number of remaining days decreases.
     *
     * @example To calculate the discount for a transaction:
     *          {{{
     *          val transaction = Transaction("2023-04-18T18:18:40Z", "Milk", "2023-05-10", 10, 2.99, "Store", "Visa")
     *          val discount = qualifyExpireDay(transaction)
     *          }}}
     *
     * @throws DateTimeParseException if the `timestamp` or `expiryDate` cannot be parsed.

     */
    private def qualifyExpireDay(trx: Transaction, writer: PrintWriter) :Int = {
        try {
            writeLog(writer, f"${trx.timestamp}: Qualifying Expiry date.", "Info")
            // Specify the format for the timestamp field and parse it as a LocalDate object
            val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
            val sellDate: LocalDate = LocalDate.parse(trx.timestamp, formatter)
            // Parses the expiry date (it has the default format) as a LocalDate object
            val expiryDate: LocalDate = LocalDate.parse(trx.expiryDate)
            // Calculate the days remaining until expiry for the product
            val daysRemaining: Int = (expiryDate.toEpochDay - sellDate.toEpochDay).toInt
            // return the proper discount (30 - 29 days = 1 %, 30 - 25 days = 5%, and so on)
            if (daysRemaining > 29) 0 else 30 - daysRemaining
        }
        catch {
            // In case of invalid date format the order will be considered as not qualified for discount
            case e: DateTimeParseException =>
                writeLog(writer, f"${trx.timestamp}: Invalid date format!.", "Error")
                0
        }
    }

    /**
     * Determines the discount based on the product's name. This function checks if the product name includes
     * specific keywords ("wine" or "cheese") and assigns a discount accordingly. The discount is set at 5% for wine
     * and 10% for cheese.
     * There is no metadata to determine that the product category is specified at the beginning of its name,
     * so a more general approach was followed!
     * <p> This function is instrumental in promoting sales of certain categories of products by applying a
     * higher discount rate, encouraging consumers to purchase more of these items.
     *
     * @param trx The transaction containing the product details. The product name in the transaction is evaluated
     *            to determine the discount.
     * @param writer A Print Writer object to be used to write log entries.
     * @return The discount rate as an integer percentage. If the product name contains "wine", it returns 5;
     *         if it contains "cheese", it returns 10; otherwise, it returns 0.
     * @example To get the discount for a transaction involving wine:
     *          {{{
     *          val transaction = Transaction("2023-04-18T18:18:40Z", "Wine - Red", "2023-12-31", 5, 15.50, "Online", "PayPal")
     *          val discount = qualifyProduct(transaction)
     *          }}}
     */
    private def qualifyProduct(trx: Transaction, writer: PrintWriter) =
        // If the product name contains the ord wine, it is a wine product! Same for Cheese, other wise
        // the product is not qualified for a discount
        writeLog(writer, f"${trx.timestamp}: Qualifying Product Category.", "Info")
        trx.productName.toLowerCase() match  {
            case name if name.startsWith("wine") => 5
            case name if name.startsWith("cheese") => 10
            case _ =>  0
        }

    /**
     * Determines a significant discount if the sale happens on a specific day, the 23rd of March. This function
     * checks the date in the transaction's timestamp and applies a 50% discount if it matches the target date,
     * which can be a strategic decision to boost sales on that particular day.
     *
     * <p> This type of discount can be used for special sales events or annual promotions that occur on a fixed date.
     *
     * @param trx The transaction that includes the timestamp when the sale was made. The timestamp is evaluated
     *            to check if the transaction occurred on March 23rd.
     * @param writer A Print Writer object to be used to write log entries.
     * @return The discount percentage. Returns 50 if the transaction date is March 23rd; otherwise, it returns 0.
     * @example To calculate the discount for a transaction on March 23rd:
     * {{{
     *          val transaction = Transaction("2023-03-23T11:00:00Z", "Laptop", "2023-06-10", 1, 1200.00, "Store", "Credit Card")
     *          val discount = qualifySale(transaction)
     *           }}}
     */
    private def qualifySale(trx: Transaction, writer: PrintWriter): Int = {
        // Specify the pattern for orders purchased in 23rd of March
        writeLog(writer, f"${trx.timestamp}: Qualifying Special 23rd of March sale.", "Info")
        val pattern = "^\\d{4}-03-23".r
        // If the pattern matches a 50 % Discount is applied, else it is not qualified for a discount
        if (pattern.findFirstIn(trx.timestamp).isDefined) 50 else 0
    }

    /**
     * Determines a discount based on the quantity of the product purchased. This function applies a tiered discount system
     * where higher quantities lead to larger discounts. The discounts are set as follows: 10% for quantities of 15 or more,
     * 7% for quantities between 10 and 14, 5% for quantities between 6 and 9, and no discount for quantities less than 6.
     *
     * <p> This discount strategy encourages bulk purchases by providing greater incentives for buying larger quantities.
     *
     * @param trx The transaction containing details of the product quantity. This quantity is used to determine
     *            the applicable discount rate.
     * @param writer A Print Writer object to be used to write log entries.
     * @return The discount rate as an integer percentage based on the quantity. The function returns 10 for quantities
     *         of 15 or more, 7 for 10 to 14, 5 for 6 to 9, and 0 for less than 6.
     * @example To get the discount for a transaction involving 12 units:
     * {{{
     *          val transaction = Transaction("2023-04-18T18:18:40Z", "Coffee Beans - Bulk", "2023-12-31", 12, 20.00, "Online", "Debit Card")
     *          val discount = qualifyQuantity(transaction)
     *           }}}
     */
    private def qualifyQuantity(trx: Transaction, writer: PrintWriter):Int =
        writeLog(writer, f"${trx.timestamp}: Qualifying Quantity.", "Info")
        // Calculate the discount based on the quantity sold of the product
        trx.quantity match {
            case q if q >= 15 => 10
            case q if q >= 10 => 7
            case q if q >= 6 => 5
            case _ => 0
        }

    /**
     * Calculates a channel-based discount for transactions completed through a specific application channel.
     * This method determines the discount based on the quantity of items in the transaction, with different
     * tiers of discounts applied only if the transaction is made via the "App" channel. For transactions
     * through other channels, no discount is applied.
     *
     * <p> The discount calculation is tiered such that every increment of 5 units in quantity increases the
     * discount by 5%, starting from quantities of 1 (5%), up to the maximum discount based on the available
     * quantity. For example, quantities 1-5 get a 5% discount, 6-10 get 10%, etc.
     *
     * @param trx The `Transaction` object to be evaluated, which contains details such as the channel of the
     *            transaction and the quantity of items purchased.
     * @param writer A Print Writer object to be used to write log entries.
     * @return An integer representing the discount percentage. If the transaction is through the "App" channel,
     *         returns a discount based on quantity tiers; otherwise, returns 0.
     * @example Usage:
     * {{{
     *          val transaction = Transaction("2023-04-18T18:18:40Z", "Smartphone", "2024-01-01", 7, 300.00, "App", "Credit Card")
     *          val discount = qualifyChannel(transaction)
     *          // discount should be 10 as the quantity falls within the 6-10 range
     *           }}}
     * @note This method specifically handles transactions made through the "App" channel, ensuring promotional
     *       discounts are applied to stimulate sales via this platform. For other channels, it enforces no discount policy.
     */
    private def qualifyChannel(trx: Transaction, writer: PrintWriter): Int = {
        writeLog(writer, f"${trx.timestamp}: Qualifying Payment Channel.", "Info")
        // Map 0-4 to 0, 5-9 to 1, 10-14 to 2, and so on
        val qualifier: Int = (trx.quantity - 1) / 5
        // Map 0 to 5, 1 to 10, 2 to 15 and so on (for App Transactions Only).
        if (trx.channel.equals("App")) (qualifier + 1) * 5 else 0
    }

    /**
     * Determines a discount based on the payment method specified in a transaction. This function awards a fixed
     * discount if the payment method is "Visa"; otherwise, no discount is applied.
     *
     * <p> This discount incentive is designed to promote the use of Visa cards, potentially due to partnership agreements
     * or marketing strategies aimed at encouraging customers to use Visa over other payment methods.
     *
     * @param trx The `Transaction` object containing details of the transaction including the payment method.
     * @param writer A Print Writer object to be used to write log entries.
     * @return An integer representing the discount percentage. If the payment method is "Visa", returns 5;
     *         otherwise, returns 0.
     * @example Usage:
     * {{{
     *          val transaction = Transaction("2023-04-18T18:18:40Z", "Laptop", "2024-01-01", 1, 1200.00, "Online", "Visa")
     *          val discount = qualifyMethod(transaction)
     *          // discount should be 5 as the payment method is Visa
     * }}}
     * @note This method is particularly useful for scenarios where specific promotions are tied to payment methods.
     *       It can easily be extended to include additional payment methods and corresponding discounts.
     */
    private def qualifyMethod(trx: Transaction, writer: PrintWriter) = {
        writeLog(writer, f"${trx.timestamp}: Qualifying Payment Method.", "Info")
        if (trx.paymentMethod.equals("Visa")) 5 else 0
    }

    /**
     * Determines whether a given transaction qualifies for a discount based on a specified rule.
     * This method evaluates a transaction using a provided discount rule function. It checks if the
     * result of applying the rule to the transaction is non-zero, which indicates that the transaction
     * qualifies for some form of discount under that rule.
     *
     * <p> This utility function is a part of a larger decision-making process where different rules
     * may be applied to determine the applicability of various discounts, promotions, or qualifications
     * within a retail environment.
     *
     * @param rule A function that takes a `Transaction` and returns an `Int`. The function represents
     *             a discount rule which, when applied to a transaction, yields an integer value indicating
     *             the discount percentage. A return value of `0` indicates no discount.
     * @param trx  The `Transaction` to be evaluated by the rule. This object contains all necessary
     *             details about the transaction such as product name, quantity, and price.
     * @return `true` if the transaction qualifies for the discount (i.e., the rule function returns
     *         a non-zero value); `false` otherwise.
     * @example Usage:
     * {{{
     *          val transaction = Transaction("2023-04-18T18:18:40Z", "Cheese", "2023-05-10", 10, 3.50, "Online", "Credit Card")
     *          val discountRule = qualifyProduct  // assume qualifyProduct is a function defined elsewhere
     *          val isDiscounted = isQualified(discountRule, transaction)
     *           }}}
     * @note This method assumes that all rules are implemented such that a return value of `0` strictly indicates
     *       no qualification for discounts, and any non-zero value indicates qualification.
     */
    private def isQualified(rule: Transaction => Int, trx: Transaction): Boolean = rule(trx) > 0

    /**
     * Aggregates discounts from a list of discount-qualifying functions applied to a given transaction. Each function
     * in the list is called with the transaction as an argument, and the resulting discounts are collected into a list.
     *
     * <p> This method allows for modular addition of discount rules and easy computation of all applicable discounts
     * on a transaction, facilitating the application of multiple discount policies simultaneously.
     *
     * @param rules A list of functions, each of which takes a `Transaction` object and returns an `Int` representing
     *                  a discount percentage.
     * @param trx       The transaction to which the discount functions are applied.
     * @param writer A Print Writer object to be used to write log entries.
     * @return A list of integers, each representing a discount percentage obtained from the corresponding function
     *         in the `functions` list.
     * @example To calculate all discounts for a specific transaction:
     * {{{
     *          val transaction = Transaction("2023-04-18T18:18:40Z", "Chocolate Bar", "2024-01-01", 25, 2.50, "Store", "Cash")
     *          val discountFunctions = List(qualifyExpireDay, qualifyProduct, qualifySale, qualifyQuantity)
     *          val discounts = getDiscounts(discountFunctions, transaction)
     *           }}}
     */
    private def getDiscounts(rules: List[(Transaction, PrintWriter) => Int],
                             trx: Transaction, writer: PrintWriter): List[Int] = rules.map(_(trx, writer))

    /**
     * Normalizes the calculated discounts to ensure a maximum and fair application of multiple discounts. The function
     * first sorts the discounts in descending order to prioritize higher discounts. It then calculates the final
     * discount based on the top two discounts. If only one discount is significant (non-zero), it is returned as is.
     * If two discounts are significant, their normalized sum is calculated and returned.
     *
     * <p> This method ensures that the discount application is balanced and prevents disproportionate discount stacking
     * which might otherwise lead to unrealistic pricing.
     *
     * @param discounts A list of integer values representing individual calculated discounts for a transaction.
     * @return A double representing the normalized and potentially compounded discount to be applied.
     * @example To normalize a set of discounts:
     * {{{
     *          val discounts = List(10, 7, 5, 3)
     *          val normalizedDiscount = normalizeDiscounts(discounts) // => .085
     *           }}}
     */
    private def normalizeDiscounts(discounts: List[Int]): Double = {
        if (discounts.length > 2){
            // Sort the discounts e.g. discounts [0, 5, 15, 0] => [15, 5, 0, 0]
            val sortedDiscounts: List[Int] = discounts.sortBy(_ * -1)
            // If the product is qualified to only a single discount and the others are zeros return the discount percentage.
            // If the product is not qualified to any discounts it would return 100/0 = 0
            if (sortedDiscounts(1) == 0) sortedDiscounts.head / 100.0
            /* If the product is qualified to 2 or more products, get the average of the top 2.
             * The typical approach would be ceil(sumDiscount / 2) / 100 so discounts of 10, 5 are ceil(7.5) / 100 = .08
             * but instead I used ceil(sumDiscount / .2) /1000 so discount of 10, 5 are ceil (75) / 100 which is .075
             * This allows for a higher precision nothing more!
             */
            else math.ceil((sortedDiscounts.head + sortedDiscounts(1)) / .2) / 1000
        }
        // If only one discount is applied return its value
        else if(discounts.length == 1) discounts.head / 100.0
        else 0 // Println(Error)
    }

    /**
     * Calculates the total discount for a transaction based on a set of discount-qualifying functions. This method
     * applies each rule to the transaction and uses `normalizeDiscounts` to calculate a final, combined discount percentage.
     *
     * <p> This is a crucial method for the dynamic pricing model, allowing the application of multiple promotional and
     * pricing strategies to a single transaction.
     *
     * @param trx The transaction for which the discount is to be calculated.
     * @param writer A Print Writer object to be used to write log entries.
     * @return A double representing the total discount percentage for the transaction.
     * @example To calculate the total discount for a transaction:
     * {{{
     *          val transaction = Transaction("2023-04-18T18:18:40Z", "Notebook", "2024-01-01", 10, 3.50, "Online", "Credit Card")
     *          val totalDiscount = calcDiscount(transaction)
     *           }}}
     */
    private def calcDiscount(trx: Transaction, writer: PrintWriter): Double = {
        writeLog(writer, f"Applying the discount rules for ${trx.timestamp}", "Info")
        // Specify the set of rules to apply on a transaction
        val rules: List[(Transaction, PrintWriter) => Int] = List(qualifyExpireDay, qualifyProduct,
                                                   qualifySale, qualifyQuantity, qualifyChannel, qualifyMethod)
        // Get the discounts corresponding to those rules
        val discounts :List[Int] = getDiscounts(rules, trx, writer)
        // Normalize the value to the final discount
        writeLog(writer, f"Getting the deserved discount for ${trx.timestamp}", "Info")
        normalizeDiscounts(discounts)
    }

    /**
     * Process each transaction. This method computes the
     * discount using `calcDiscount` and then applies it to the total price of the transaction, providing a new
     * `ProcessedTransaction` with the applied discount and total due.
     *
     * <p> This method is the final step in processing a transaction, encapsulating all discount rules and their application
     * into a single actionable function, making it easy to apply complex pricing models to sales data.
     *
     * @param trx The transaction to process with discount rules.
     * @param writer A Print Writer object to be used to write log entries.
     * @return A `ProcessedTransaction` that includes the original transaction details along with the calculated discount
     *         and the final price after discount.
     * @example To process a transaction with all applicable rules:
     * {{{
     *          val transaction = Transaction("2023-04-18T18:18:40Z", "Printer Ink", "2024-01-01", 2, 50.00, "Store", "Cash")
     *          val processedTransaction = processTransaction(transaction)
     *           }}}
     */
    private def processTransaction(trx: Transaction, writer: PrintWriter): ProcessedTransaction = {
        writeLog(writer, f"Calculating the discount for ${trx.timestamp}", "Info")
        // Calculate the discount for the transaction
        val discount: Double = calcDiscount(trx, writer)
        // Calculate its final price
        writeLog(writer, f"Calculating total due for ${trx.timestamp}", "Info")
        val finalPrice: Double = trx.unitPrice * trx.quantity * (1 - discount)
        // Return the processed transaction
        ProcessedTransaction(trx, discount, finalPrice)
    }

    /**
     * Main function to operate the retail rule engine. It orchestrates the entire process of reading data from a CSV,
     * transforming it into transactions, applying business rules to compute discounts and final prices, and then
     * writing the processed results back into the database.
     *
     * <p> This function serves as the entry point for processing batches of transactions, leveraging other functions
     * to handle specific tasks in the workflow.
     *
     * @example To execute the operation process:
     * {{{
     *          operate()
     *           }}}
     * @see readData
     * @see writeData
     * @see processTransaction
     */
    private def operate(): Unit = {
        // Open log file
        val f: File = new File("src/main/resources/rules_engine_logs.txt")
        // create a print writer object => It will be used across the methods to write log lines
        val writer: PrintWriter = new PrintWriter(new FileOutputStream(f, true))

        writeLog(writer, "Rule Engine Started.", "Info")
        // Read the data from the CSV File
        writeLog(writer, "Opening the transactions file ...", "Info")
        val response: (List[String], Int) = readData("src/main/Resources/TRX1000.csv")
        val lines: List[String] = response._1
        val statusCode: Int = response._2
        // In case of an empty file
        if (statusCode == 0)  writeLog(writer, "You are trying to process un empty file!", "Warning")
        // In case of a Non Existing File
        else if (statusCode == -1)
            writeLog(writer, "There no such file in the specified path!", "Error")
        // In case of an Input Output Error
        else if (statusCode == -2)
            writeLog(writer, "An I/O Error Happened trying to read the file!", "Error")
        else {
            writeLog(writer, f"$statusCode lines read successfully!", "Info")
            // Process each line (transaction) in the file.
            // filter(_.timestamp.equals("-1")) => A timestamp of -1 represents an invalid transaction!
            val results: List[ProcessedTransaction] = lines.map(toTrx).
                                                            filterNot(_.timestamp.equals("-1")).
                                                            map(x => processTransaction(x, writer))
            // Write the processed transactions to the database
            val status: Int = writeData(results, writer)
            status match {
                // Status code 1 representing a successful operation
                case 1 => writeLog(writer, "Data successfully inserted into the Database!", "Info")
                case -1 => writeLog(writer, "An error happened trying to connect to the database" +
                    ", check your credentials and try again!", "Error")
                case -2 => writeLog(writer, "A TimeOut Error happened trying to connect to the database!" +
                    ", Check that the database server is up and running!", "Error")
                case -3 => writeLog(writer, "An error happened trying to locate the jar file for JDBC Class!", "Error")
                case _ => writeLog(writer, "Unexpected error happened trying to write the data to the database!", "Error")
            }

        }
        writeLog(writer, "Rule Engine Finished.", "Info")
        // Close the log file upon completing.
        writer.close()
    }
    // Start processing data.
    operate()
}
