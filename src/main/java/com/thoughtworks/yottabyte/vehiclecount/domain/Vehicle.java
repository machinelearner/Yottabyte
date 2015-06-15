package com.thoughtworks.yottabyte.vehiclecount.domain;

import org.apache.hadoop.io.Text;

public class Vehicle {
    private Text row;

    public Vehicle(Text row) {

        this.row = row;
    }

    public Text type() {
        return new Text(row.toString().split(",")[0]);
    }
}
