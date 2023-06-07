var selectedDB, selectedTable, columnName;

function goBack() {
    var backButton1 = document.getElementById("backBt");
    backButton1.style.display = "block";
}
function goBackToTable() {
    var backButton2 = document.getElementById("backBt2");
    backButton2.style.display = "block";
}
function clearDB(){
    const db_div = document.getElementById("database-div");
    db_div.innerHTML = "";
}
function clearTables(){
    const tb_div = document.getElementById("data-table");
    tb_div.innerHTML = "";
}
function getDatabases(){
    window.location.href = 'http://127.0.0.1:8000/home';
}

function getTables(){

    // const tableColumn = document.getElementById("html-column-data");
    // tableColumn.innerHTML = "";

    var radioButtons = document.getElementsByName("dbradio");
    // console.log(document.querySelectorAll(radioButtons.checked).length);
    for (var i = 0; i < radioButtons.length; i++) {
        if (radioButtons[i].checked){
            selectedDB = radioButtons[i].value;
            var dbName = selectedDB;
        }
    }

    const getColumnsButton = document.getElementById('getColumnsButton');
    const mytable = document.getElementById("html-data-table");
    mytable.innerHTML = "";


    var res = fetch("http://127.0.0.1:8000/" + dbName)
    .then(function(response) {
        return response.json()
    })
    .then(function(data){
        console.log(data);
        if(data.length == 0){
            alert("No tables to display,Kindly select other database.Thank you!");
        }
        else{
            renderDataInTheTable(data);
        }
    });
    function renderDataInTheTable(todos) {
        let head = document.createElement("h1");
        head.innerHTML = 'Tables for selected database '+ `<b>`+ selectedDB+`</b>`;
        head.style = "font-size: 18px";
        // create table th tag 
        let h3 = document.createElement("th");
        h3.innerText = "Table Name";
        let h1 = document.createElement("th");
        h1.innerText = "Classification";
        let h2 = document.createElement("th");
        h2.innerText = " ";

        // append table th,p tag in first row 
        let thTag = document.createElement("tr");
        mytable.appendChild(head);
        thTag.appendChild(h3);
        thTag.appendChild(h1);
        thTag.appendChild(h2);
        mytable.appendChild(thTag);
            todos.forEach(todo => {
                let newRow = document.createElement("tr");
                let radio = document.createElement("td");
                let input = document.createElement("input");
                input.name="tbradio"
                input.type = "radio";
                radio.appendChild(input);                                  
                newRow.appendChild(radio);    
                
                Object.values(todo).forEach((value) => {
                    // create input tag type radio
                    let cell = document.createElement("td");
                    cell.innerText = value;
                    newRow.appendChild(cell);
                    input.value = todo['Table Name'];
                    newRow.appendChild(radio);    

                })
                mytable.appendChild(newRow);
                // Check if the <ul> has <li> elements and show/hide the button accordingly
                    console.log(newRow.children.length);
                if (newRow.children.length > 0) {
                    getColumnsButton.style.display = 'block';
                }
            });
    }

}

function getColumns(){

    // var radioButtons = document.getElementsByName("dbradio");
    // console.log(document.querySelectorAll(radioButtons.checked).length);
    const getMaskingButton = document.getElementById('getMaskingButton');

    // for (var i = 0; i < radioButtons.length; i++) {
    //     if (radioButtons[i].checked){
    //         var selectedDB = radioButtons[i].value;
    //     }
    // }
    var radioButton = document.getElementsByName("tbradio");
    for (var j = 0; j < radioButton.length; j++) {
        if (radioButton[j].checked){
            selectedTable = radioButton[j].value;
            console.log("Tablename: ",selectedTable);
        }
    }
    const tableColumn = document.getElementById("html-column-data");
    tableColumn.innerHTML = "";

    var res = fetch("http://127.0.0.1:8000/getColumns/"+ selectedDB+'/'+ selectedTable)
    .then(function(response) {
        return response.json()
    })
    .then(function(data){
        console.log(data);
        renderDataInTheTable(data);
    });

    function renderDataInTheTable(todos) {
        let head = document.createElement("h1");
        head.innerHTML = 'Columns for selected table '+ `<b>`+ selectedTable+`</b>`;
        head.style = "font-size: 16px";
        // create table th tag 
        let h3 = document.createElement("th");
        h3.innerText = "Column Name";
        let h1 = document.createElement("th");
        h1.innerText = "Type";
        let h2 = document.createElement("th");
        h2.innerText = " ";

        // append table th,p tag in first row 
        let thTag = document.createElement("tr");
        tableColumn.appendChild(head);
        thTag.appendChild(h3);
        thTag.appendChild(h1);
        thTag.appendChild(h2);

        tableColumn.appendChild(thTag);
        todos.forEach(todo => {
            let newRow = document.createElement("tr");
            let radio = document.createElement("td");
            let input = document.createElement("input");
            input.name="colradio"
            // input.type = "radio";
            input.type = "checkbox";
            radio.appendChild(input);                                  
            newRow.appendChild(radio);
            
            Object.values(todo).forEach((value) => {
                // create input tag type radio
                let cell = document.createElement("td");
                cell.innerText = value;
                newRow.appendChild(cell);
                input.value = todo['Name'];
                newRow.appendChild(radio);    
            })
            tableColumn.appendChild(newRow);
            // Check if the <ul> has <li> elements and show/hide the button accordingly
            if (newRow.children.length > 0) {
                getMaskingButton.style.display = 'block';
            }
        });
    }
}

// Mask the selected column by running glue job

function maskColumn(){
    let arr = [];
    var radioButtons = document.getElementsByName("colradio");

    for(var c=0;c<radioButtons.length;c++){
        if (radioButtons[c].checked){
            // columnName = radioButtons[c].value;
            arr.push(radioButtons[c].value);
        }
    }
    let strings = arr.join(',');
    console.log('check box elements ',strings);
    
    var res = fetch("http://127.0.0.1:8000/"+selectedDB+"/"+selectedTable+"/"+strings)
    .then(function(response) {
        return response.json()
    })
    .then(function(data){
        console.log(data);
        // printSampleData();
        getJobRunStatus(data);
    });
}


function getJobRunStatus(data){
    document.getElementById("response").innerHTML = " " ;
    var interval = setInterval(function() { 
        var res = fetch("http://127.0.0.1:8000/get_job_status/"+selectedDB+"/"+ selectedTable+"/"+data['JobRunId'])
        .then(function(response) {
            return response.json()
        })
        .then(function(status){
            console.log("Status is:",status);
            if (status == "RUNNING") { 
                document.getElementById("response").innerHTML =`<h2 class="inline">`+"Current job status is " +status+ " "+`<h2 class="loading inline"></h2>` +`</h2>`;
            }
            else if (status == "FAILED") { 
                document.getElementById("response").innerHTML =`<h2>`+"Current job status is " +status+`</h2>` ;
                clearInterval(interval);
            }
            else{ 
                document.getElementById("response").innerHTML =`<h2>`+"Current job status is " +status+`</h2>` ;
                clearInterval(interval);
            }

            if (status == "SUCCEEDED"){
                printSampleData();
            } 
        });
    }, 5000);    

}

function printSampleData(){
    document.getElementById("response").innerHTML = " " ;

    var res = fetch("http://127.0.0.1:8000/get_sample_data/"+ selectedDB+'/'+selectedTable + '/')
        .then(function(response) {
            return response.json()
        })
        .then(function(data){
            console.log(data);
        });

        
}