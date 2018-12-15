package controller;

import node.PaxosNode;
//import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by Ajay on 14-12-2018.
 */
public class Controller {
    public static void main(String[] args) {
        int[] num = {1, 2, 3};
        List<Thread> processes = new ArrayList<Thread>();
        HashMap<Integer, PaxosNode> nodes = new HashMap<Integer, PaxosNode>();

        IntStream.range(1, 4).forEach(
//        for(int i : num)
            i -> {
                nodes.put(i, new PaxosNode(i));
            }
        );

        for(int i : num) {
            nodes.get(i).set_majority(nodes);
        }

        for(int i : num) {
            processes.add(new Thread(nodes.get(i)));
        }

        for(int i : num) {
            processes.get(i-1).start();
        }

//        Options
    }
}
