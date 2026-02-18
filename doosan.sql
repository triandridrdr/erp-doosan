-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Feb 18, 2026 at 09:19 AM
-- Server version: 10.4.32-MariaDB
-- PHP Version: 8.0.30

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `doosan`
--

-- --------------------------------------------------------

--
-- Table structure for table `journal_entries`
--

CREATE TABLE `journal_entries` (
  `id` bigint(20) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `created_by` varchar(50) DEFAULT NULL,
  `deleted` bit(1) NOT NULL,
  `deleted_at` datetime(6) DEFAULT NULL,
  `updated_at` datetime(6) DEFAULT NULL,
  `updated_by` varchar(50) DEFAULT NULL,
  `description` varchar(500) DEFAULT NULL,
  `entry_date` date NOT NULL,
  `entry_number` varchar(50) NOT NULL,
  `status` enum('CANCELLED','DRAFT','POSTED') NOT NULL,
  `total_credit` decimal(19,2) NOT NULL,
  `total_debit` decimal(19,2) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `journal_entry_lines`
--

CREATE TABLE `journal_entry_lines` (
  `id` bigint(20) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `created_by` varchar(50) DEFAULT NULL,
  `deleted` bit(1) NOT NULL,
  `deleted_at` datetime(6) DEFAULT NULL,
  `updated_at` datetime(6) DEFAULT NULL,
  `updated_by` varchar(50) DEFAULT NULL,
  `account_code` varchar(50) NOT NULL,
  `account_name` varchar(200) NOT NULL,
  `credit` decimal(19,2) NOT NULL,
  `debit` decimal(19,2) NOT NULL,
  `description` varchar(500) DEFAULT NULL,
  `line_number` int(11) NOT NULL,
  `journal_entry_id` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `sales_orders`
--

CREATE TABLE `sales_orders` (
  `id` bigint(20) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `created_by` varchar(50) DEFAULT NULL,
  `deleted` bit(1) NOT NULL,
  `deleted_at` datetime(6) DEFAULT NULL,
  `updated_at` datetime(6) DEFAULT NULL,
  `updated_by` varchar(50) DEFAULT NULL,
  `customer_code` varchar(50) NOT NULL,
  `customer_name` varchar(200) NOT NULL,
  `delivery_address` varchar(500) DEFAULT NULL,
  `order_date` date NOT NULL,
  `order_number` varchar(50) NOT NULL,
  `remarks` varchar(1000) DEFAULT NULL,
  `status` enum('CANCELLED','CONFIRMED','PENDING','SHIPPED') NOT NULL,
  `total_amount` decimal(19,2) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `sales_orders`
--

INSERT INTO `sales_orders` (`id`, `created_at`, `created_by`, `deleted`, `deleted_at`, `updated_at`, `updated_by`, `customer_code`, `customer_name`, `delivery_address`, `order_date`, `order_number`, `remarks`, `status`, `total_amount`) VALUES
(1, '2026-02-18 13:12:46.000000', 'system', b'0', NULL, '2026-02-18 13:12:46.000000', 'system', 'cust-01', 'Fransiva', 'alamat', '2026-02-18', 'SO-2026-1001', '', 'PENDING', 0.00),
(2, '2026-02-18 13:13:45.000000', 'system', b'0', NULL, '2026-02-18 13:13:45.000000', 'system', 'cust2', 'test', '', '2026-02-18', 'SO-2026-1002', '', 'PENDING', 0.00);

-- --------------------------------------------------------

--
-- Table structure for table `sales_order_lines`
--

CREATE TABLE `sales_order_lines` (
  `id` bigint(20) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `created_by` varchar(50) DEFAULT NULL,
  `deleted` bit(1) NOT NULL,
  `deleted_at` datetime(6) DEFAULT NULL,
  `updated_at` datetime(6) DEFAULT NULL,
  `updated_by` varchar(50) DEFAULT NULL,
  `item_code` varchar(50) NOT NULL,
  `item_name` varchar(200) NOT NULL,
  `line_amount` decimal(19,2) NOT NULL,
  `line_number` int(11) NOT NULL,
  `quantity` decimal(19,2) NOT NULL,
  `remarks` varchar(500) DEFAULT NULL,
  `unit_price` decimal(19,2) NOT NULL,
  `sales_order_id` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `sales_order_lines`
--

INSERT INTO `sales_order_lines` (`id`, `created_at`, `created_by`, `deleted`, `deleted_at`, `updated_at`, `updated_by`, `item_code`, `item_name`, `line_amount`, `line_number`, `quantity`, `remarks`, `unit_price`, `sales_order_id`) VALUES
(1, '2026-02-18 13:12:46.000000', 'system', b'0', NULL, '2026-02-18 13:12:46.000000', 'system', 'test', 'nama 1', 0.00, 1, 1.00, '', 0.00, 1),
(2, '2026-02-18 13:12:46.000000', 'system', b'0', NULL, '2026-02-18 13:12:46.000000', 'system', 'test', 'nama 2', 0.00, 2, 1.00, '', 0.00, 1),
(3, '2026-02-18 13:13:45.000000', 'system', b'0', NULL, '2026-02-18 13:13:45.000000', 'system', 'code', 'name', 0.00, 1, 1.00, '', 0.00, 2);

-- --------------------------------------------------------

--
-- Table structure for table `stocks`
--

CREATE TABLE `stocks` (
  `id` bigint(20) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `created_by` varchar(50) DEFAULT NULL,
  `deleted` bit(1) NOT NULL,
  `deleted_at` datetime(6) DEFAULT NULL,
  `updated_at` datetime(6) DEFAULT NULL,
  `updated_by` varchar(50) DEFAULT NULL,
  `available_quantity` decimal(19,2) NOT NULL,
  `item_code` varchar(50) NOT NULL,
  `item_name` varchar(200) NOT NULL,
  `quantity` decimal(19,2) NOT NULL,
  `reserved_quantity` decimal(19,2) NOT NULL,
  `unit` varchar(20) NOT NULL,
  `unit_price` decimal(19,2) DEFAULT NULL,
  `warehouse_code` varchar(50) NOT NULL,
  `warehouse_name` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `stocks`
--

INSERT INTO `stocks` (`id`, `created_at`, `created_by`, `deleted`, `deleted_at`, `updated_at`, `updated_by`, `available_quantity`, `item_code`, `item_name`, `quantity`, `reserved_quantity`, `unit`, `unit_price`, `warehouse_code`, `warehouse_name`) VALUES
(1, '2026-02-18 13:40:51.000000', 'system', b'0', NULL, '2026-02-18 13:40:51.000000', 'system', 2.00, 'item', 'bahan katun', 2.00, 0.00, 'EA', 200.00, 'wh-01', 'warehouse Bandungh');

-- --------------------------------------------------------

--
-- Table structure for table `users`
--

CREATE TABLE `users` (
  `id` bigint(20) NOT NULL,
  `created_at` datetime(6) NOT NULL,
  `created_by` varchar(50) DEFAULT NULL,
  `deleted` bit(1) NOT NULL,
  `deleted_at` datetime(6) DEFAULT NULL,
  `updated_at` datetime(6) DEFAULT NULL,
  `updated_by` varchar(50) DEFAULT NULL,
  `name` varchar(50) NOT NULL,
  `password` varchar(255) NOT NULL,
  `role` enum('ADMIN','USER') NOT NULL,
  `user_id` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `users`
--

INSERT INTO `users` (`id`, `created_at`, `created_by`, `deleted`, `deleted_at`, `updated_at`, `updated_by`, `name`, `password`, `role`, `user_id`) VALUES
(1, '2026-02-18 11:25:59.000000', NULL, b'0', NULL, '2026-02-18 11:25:59.000000', NULL, 'Administrator', '$2a$10$7EqJtq98hPqEX7fNZaFWoOhi5Q3nYj0VvFvYh0R5p2q5kYvE8k2b6', 'ADMIN', 'admin'),
(2, '2026-02-18 13:11:00.000000', 'system', b'0', NULL, '2026-02-18 13:11:00.000000', 'system', 'admin2', '$2a$10$jHW.Xr/aChYOQrv/gSc8je6hVnP9jjbS35OG4AO76/Pz0KOqvX0oG', 'USER', 'admin2');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `journal_entries`
--
ALTER TABLE `journal_entries`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `UK6vtg8oj18vbcqphqfokc7qtvf` (`entry_number`);

--
-- Indexes for table `journal_entry_lines`
--
ALTER TABLE `journal_entry_lines`
  ADD PRIMARY KEY (`id`),
  ADD KEY `FKl0yqkqwya5s96hmxy9hq2c8yi` (`journal_entry_id`);

--
-- Indexes for table `sales_orders`
--
ALTER TABLE `sales_orders`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `UK710tqn7k0rkp3ubriqlh0woyp` (`order_number`);

--
-- Indexes for table `sales_order_lines`
--
ALTER TABLE `sales_order_lines`
  ADD PRIMARY KEY (`id`),
  ADD KEY `FKtgqomomuqhk8i92lc9hjr7j71` (`sales_order_id`);

--
-- Indexes for table `stocks`
--
ALTER TABLE `stocks`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `users`
--
ALTER TABLE `users`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `UK6efs5vmce86ymf5q7lmvn2uuf` (`user_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `journal_entries`
--
ALTER TABLE `journal_entries`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `journal_entry_lines`
--
ALTER TABLE `journal_entry_lines`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `sales_orders`
--
ALTER TABLE `sales_orders`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- AUTO_INCREMENT for table `sales_order_lines`
--
ALTER TABLE `sales_order_lines`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- AUTO_INCREMENT for table `stocks`
--
ALTER TABLE `stocks`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT for table `users`
--
ALTER TABLE `users`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `journal_entry_lines`
--
ALTER TABLE `journal_entry_lines`
  ADD CONSTRAINT `FKl0yqkqwya5s96hmxy9hq2c8yi` FOREIGN KEY (`journal_entry_id`) REFERENCES `journal_entries` (`id`);

--
-- Constraints for table `sales_order_lines`
--
ALTER TABLE `sales_order_lines`
  ADD CONSTRAINT `FKtgqomomuqhk8i92lc9hjr7j71` FOREIGN KEY (`sales_order_id`) REFERENCES `sales_orders` (`id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
