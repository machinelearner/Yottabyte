package com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain;


import org.apache.hadoop.io.Text;

public class VehicleRepair {
    private String vehicleRepairRow;
    private String vehicleType;
    private String repairCode;
    private String description;
    private String currency;
    private String price;

    public VehicleRepair(String vehicleRepairRow) {

        this.vehicleRepairRow = vehicleRepairRow;
        parse(vehicleRepairRow);
    }

    private void parse(String vehicleRepairRow) {
        String[] columns = vehicleRepairRow.split(",");
        vehicleType = columns[0];
        repairCode = columns[1];
        description = columns[2];
        currency = columns[3];
        price = columns[4];
    }

    public Text code() {
        return new Text(repairCode);
    }

    public Text description() {
        return new Text(description);
    }

    public Text vehicleType() {
        return new Text(vehicleType);
    }
}
