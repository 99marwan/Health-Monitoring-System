import classes from "./components/Button.module.css";
import "./App.css";
import React, { useState, useEffect } from "react";
import { DataGrid } from "@mui/x-data-grid";
import TextField from "@mui/material/TextField";
import AdapterDateFns from "@mui/lab/AdapterDateFns";
import LocalizationProvider from "@mui/lab/LocalizationProvider";
import DatePicker from "@mui/lab/DatePicker";
import MobileDatePicker from '@mui/lab/MobileDatePicker';
import DateTimePicker from '@mui/lab/DateTimePicker';

function App() {
  const [from, setFrom] = useState(null);
  const [to, setTo] = useState(null);
  const [data, setData] = useState([]);

  useEffect(() => {
    document.title = "Bigdata Statistics";
  }, []);

  const buttonHandler = (event) => {
    event.preventDefault();
    var fromDate = from.getTime() / 1000
    var toDate = to.getTime() / 1000
    let url = "http://localhost:8080/fireMapReduce/mapreduce/"+fromDate+"/"+toDate+"/";
    fetch(url, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then(async (res) => {
        return res.json();
      })
      .then((res) => {
        setData(res);
        console.log(res);
      });
  };
  return (
    <div className="App">
      <header className="App-header">
        <button className={classes.button} onClick={buttonHandler}>
          {" "}
          Get Statistics !{" "}
        </button>
        <div className={classes.date}>
          <span className={classes.from}>
            <LocalizationProvider dateAdapter={AdapterDateFns}>
            <DateTimePicker
              label="To"
              value={from}
              onChange={(newValue) => {
                setFrom(newValue);
              }}
              renderInput={(params) => <TextField {...params} />}
            />
            </LocalizationProvider>
          </span>
          <LocalizationProvider dateAdapter={AdapterDateFns}>
            <DateTimePicker
              label="To"
              value={to}
              onChange={(newValue) => {
                setTo(newValue);
              }}
              renderInput={(params) => <TextField {...params} />}
            />
          </LocalizationProvider>
        </div>
        <div className={classes.style2}>
          <DataGrid
            columns={[
              { field: "serviceName" },
              { field: "cpu" },
              { field: "ram" },
              { field: "disk" },
              { field: "maxCPU" },
              { field: "maxRAM" },
              { field: "maxDisk" },
              { field: "count" },
            ]}
            rows={data}
          />
        </div>
      </header>
    </div>
  );
}

export default App;
