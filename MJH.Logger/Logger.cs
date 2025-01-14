﻿using MJH.Factories;
using MJH.Interfaces;
using MJH.Models;
using System;
using System.Collections.Generic;
using MJH.BusinessLogic.Configuration;
using System.IO;

namespace MJH
{
    public static class Logger
    {
        private static ILogger LoggerInterface;
        private static ILogReader LogReader;
        private static ILogReaderV2 LogReaderV2;
        private static ITransaction TransactionInterface;

        static Logger()
        {
            var loggerFactory = new LoggerFactory();
            LoggerInterface = loggerFactory.GetLoggerRepository();

            var logReaderFactory = new LogReaderFactory();
            LogReader = logReaderFactory.GetLogReaderRepository();
            LogReaderV2 = logReaderFactory.GetLogReaderV2Repository();

            var transactionFactory = new TransactionFactory();
            TransactionInterface = transactionFactory.GetTransactionRepository();
        }

        public static void TestLogFile()
        {
            LoggerConfig config = new ConfigurationHandler().Read();
            if (config.LoggerType == LoggingTypeModel.LogOutputType.SQLite)
            {
                var dbLocation = config.SQLite.ServerInformation.LogFileLocation;
                var dbName = config.SQLite.ServerInformation.LogFileName.Insert(config.SQLite.ServerInformation.LogFileName.Length - 3, "-" + DateTime.Now.Date.ToString("yyyy-MM-dd"));

                var dbFile = new FileInfo(dbLocation + "\\" + dbName);
                if (!dbFile.Exists)
                {
                    var loggerFactory = new LoggerFactory();
                    LoggerInterface = loggerFactory.GetLoggerRepository();

                    var logReaderFactory = new LogReaderFactory();
                    LogReader = logReaderFactory.GetLogReaderRepository();
                    LogReaderV2 = logReaderFactory.GetLogReaderV2Repository();

                    var transactionFactory = new TransactionFactory();
                    TransactionInterface = transactionFactory.GetTransactionRepository();
                }
            }
        }

        /// <summary>
        /// Logs a new record to the Log Store as a type Error with the given exception.
        /// </summary>
        /// <param name="logCategory"></param>
        /// <param name="exception"></param>
        public static void LogError(LoggingTypeModel.LogCategory logCategory, Exception exception)
        {
            try
            {
                Console.WriteLine($"{DateTime.Now} - {logCategory} - {exception}");
                LoggerInterface.LogError(logCategory, exception);
            }
            catch
            {
            }
        }

        /// <summary>
        /// Logs a new record to the Log Store as a type Error with the given exception.
        /// </summary>
        /// <param name="logCategory"></param>
        /// <param name="exception"></param>
        public static void LogInfo(LoggingTypeModel.LogCategory logCategory, Exception exception)
        {
            try
            {
                Console.WriteLine($"{DateTime.Now} - {logCategory} - {exception}");
                LoggerInterface.LogInfo(logCategory, exception);
            }
            catch
            {
            }

        }

        /// <summary>
        /// Logs a new record to the Log Store as a type Error with the given exception.
        /// </summary>
        /// <param name="logCategory"></param>
        /// <param name="exception"></param>
        public static void LogDebug(LoggingTypeModel.LogCategory logCategory, Exception exception)
        {
            try
            {
                Console.WriteLine($"{DateTime.Now} - {logCategory} - {exception}");
                LoggerInterface.LogDebug(logCategory, exception);
            }
            catch
            {
            }

        }

        /// <summary>
        /// Logs a new record to the Log Store as a type Error with the given String Message.
        /// </summary>
        /// <param name="logCategory"></param>
        /// <param name="message"></param>
        public static void LogError(LoggingTypeModel.LogCategory logCategory, string message)
        {
            try
            {
                Console.WriteLine($"{DateTime.Now} - {logCategory} - {message}");
                LoggerInterface.LogError(logCategory, message);
            }
            catch
            {
            }
        }

        /// <summary>
        /// Logs a new record to the Log Store as a type Error with the given String Message.
        /// </summary>
        /// <param name="logCategory"></param>
        /// <param name="message"></param>
        public static void LogInfo(LoggingTypeModel.LogCategory logCategory, string message)
        {
            try
            {
                Console.WriteLine($"{DateTime.Now} - {logCategory} - {message}");
                LoggerInterface.LogInfo(logCategory, message);
            }
            catch
            {
            }

        }

        /// <summary>
        /// Logs a new record to the Log Store as a type Error with the given String Message.
        /// </summary>
        /// <param name="logCategory"></param>
        /// <param name="message"></param>
        public static void LogDebug(LoggingTypeModel.LogCategory logCategory, string message)
        {
            try
            {
                Console.WriteLine($"{DateTime.Now} - {logCategory} - {message}");
                LoggerInterface.LogDebug(logCategory, message);
            }
            catch
            {
            }
        }

        /// <summary>
        /// Logs a new record to the Log Store as a type Transaction with the given String Message.
        /// </summary>
        /// <param name="sourceId"></param>
        /// <param name="logMessage"></param>
        public static void LogTransaction(string sourceId, string logMessage)
        {
            try
            {
                Console.WriteLine($"{DateTime.Now} - {sourceId} - {logMessage}");
                TransactionInterface.LogTransaction(DateTime.Now, sourceId, logMessage);
            }
            catch(Exception exception)
            {
            }
        }

        /// <summary>
        /// Reads all Logged Errors from the Log Store.
        /// </summary>
        /// <returns>IReadonlyCollection</returns>
        public static IReadOnlyCollection<Error> Read()
        {
            try
            {
                return LogReader.Read();
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Reads the Last X Logged Errors from the Log Store ordered by Date and Time Logged in Descending order.  Not available if using the TextFile logger.
        /// </summary>
        /// <param name="recordCount"></param>
        /// <returns>IReadOnlyCollection</returns>
        public static IReadOnlyCollection<Error> Read(int recordCount)
        {
            try
            {
                return LogReaderV2.ReadMaxRecordCount(recordCount);
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Read all Logged Errors between given Dates and Times. Not available if using the TextFile logger.
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns>IReadOnlyCollection</returns>
        public static IReadOnlyCollection<Error> Read(DateTime startDate, DateTime endDate)
        {
            var log = LogReaderV2.ReadBetweenDates(startDate, endDate);

            return log;
        }

        /// <summary>
        /// Returns all Logged Errors by given Log Category Type.  Not available if using the TextFile logger.
        /// </summary>
        /// <param name="logCategory"></param>
        /// <returns>IReadOnlyCollection</returns>
        public static IReadOnlyCollection<Error> Read(LoggingTypeModel.LogCategory logCategory)
        {
            var log = LogReaderV2.ReadSpecificCategory(logCategory);

            return log;
        }
    }
}