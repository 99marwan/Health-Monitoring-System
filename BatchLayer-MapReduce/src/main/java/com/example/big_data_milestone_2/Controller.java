package com.example.big_data_milestone_2;


import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.sql.SQLException;

@CrossOrigin
@RestController
@RequestMapping(value="/fireMapReduce")
public class Controller {

    @GetMapping(value = "/mapreduce/{Start}/{End}")
    public String Fire(@PathVariable(name = "Start") String Start,@PathVariable(name = "End") String End) throws IOException, InterruptedException, ClassNotFoundException, SQLException {

        return Component.getResults(Start,End);
    }

}
