package node;


import java.text.MessageFormat;
import java.util.*;
import java.util.logging.Logger;

class Tuple<X, Y, Z> {
    public final X x;
    public final Y y;
    public final Z z;

    public Tuple(X x, Y y, Z z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public static String viewTuple(List<Tuple<Integer, Integer, Integer>> tuplelist) {
        Tuple t;

        String msg = "[";

        for (int i = 0; i < tuplelist.size(); i++) {
            t = tuplelist.get(i);
            if (t == null) {
                msg += "null, ";
            } else {
                msg += MessageFormat.format("({0}, {1}, {2}), ", t.x, t.y, t.z);
            }
        }
        msg += "]";

        return msg;
    }
}


public class PaxosNode implements Runnable {
    private int id;
    private Map<Integer, PaxosNode> nodes = new HashMap<Integer, PaxosNode>();

    private int majority;
    private int paxos_proposed_id;
    private int paxos_promised_id;
    private int paxos_proposed_value;
    private int paxos_accepted_value;
    private int paxos_accepted_id;
    private int num_nodes;
    private Map<Integer, HashMap<Integer, Integer>> data_store = new HashMap<Integer, HashMap<Integer, Integer>>();

    public PaxosNode(int id) {
        this.id = id;

        this.paxos_proposed_id = -1;
        this.paxos_promised_id = -1;
        this.paxos_proposed_value = -1;
        this.paxos_accepted_value = -1;
        this.paxos_accepted_id = -1;

        HashMap<Integer, Integer> hmap = new HashMap<Integer, Integer>();
        hmap.put(0, 0);
        this.data_store.put(0, hmap);
    }

    public void set_majority(HashMap<Integer, PaxosNode> nodes) {
        this.nodes = nodes;

        this.majority = (this.nodes.size() / 2) + 1;
        this.num_nodes = this.nodes.size();
    }

    private void generate_next_paxos_id() {
        /*
         * generates the next unique paxos-id for a node
         */
        if (this.paxos_proposed_id == -1) {
            this.paxos_proposed_id = this.id;
        } else {
            this.paxos_proposed_id = this.paxos_proposed_id + this.num_nodes;
        }
    }

    public Tuple<Integer, Integer, Integer> prepare(int proposed_id) {
        /*
         * receives the prepare - ID message by other paxos -nodes
         * param: proposed_id paxos - id proposed by another node
         * return {returns values/None, see code
         */

        if (proposed_id <= this.paxos_promised_id) {
            return null;
        } else {
            this.paxos_promised_id = proposed_id;

            if (this.paxos_accepted_id != -1) {
                return new Tuple<Integer, Integer, Integer>(
                        this.paxos_promised_id, this.paxos_accepted_id, this.paxos_accepted_value);
            } else {
                return new Tuple<Integer, Integer, Integer>(
                        this.paxos_promised_id, null, null);
            }
        }
    }

    public void send_prepares() {
        /*
         * Sends the prepare-ID message to every other
         * paxos-node available. If in-case the majority
         * is not achieved for proposed-ID, retry is done
         * using higher ID.
         */

        List<Tuple<Integer, Integer, Integer>> responses = new ArrayList<Tuple<Integer, Integer, Integer>>();
        this.generate_next_paxos_id();

        for (Map.Entry<Integer, PaxosNode> pair : this.nodes.entrySet()) {
            int _id = pair.getKey();
            PaxosNode node = pair.getValue();

            System.out.println(MessageFormat.format(
                    "node {0}, process {1}: sending PREPARE(id {2}) to node id {3}",
                    this.id, Thread.currentThread().getId(), this.paxos_proposed_id, node.id
            ));

            responses.add(node.prepare(this.paxos_proposed_id));
        }

        System.out.println(MessageFormat.format(
                "node {0}, process {1}: checking for majority for proposed-id {2}, " +
                        "responses ", this.id, Thread.currentThread().getId(), this.paxos_proposed_id) +
                Tuple.viewTuple(responses)
        );

        if (this.check_majority(responses)) {
            System.out.println(MessageFormat.format(
                    "node {0}, process {1}: majority for proposed-id {2} " +
                            "achieved, sending accept-requests", this.id, Thread.currentThread().getId(),
                    this.paxos_proposed_id
            ));

            this.send_accept_requests(responses);

        } else {
            this.send_prepares();
        }
    }

    public Tuple<Integer, Integer, Integer> accept_request(int paxos_proposed_id, int paxos_proposed_value) {
        /*
         * receives the accept-request message from other paxos-nodes
         */
        if (paxos_proposed_id < this.paxos_promised_id) {
            return null;
        } else {
            this.paxos_accepted_id = paxos_proposed_id;
            this.paxos_accepted_value = paxos_proposed_value;
        }
        return new Tuple<Integer, Integer, Integer>(
                this.paxos_accepted_id, this.paxos_accepted_value, null);
    }

    private void send_accept_requests(List<Tuple<Integer, Integer, Integer>> responses) {
        /*
         * Sends the accept-request message to other paxos-nodes, and if the
         * majority is not achieved for an accept-request, prepare-ID message
         * is initiated with higher ID.
         * {param responses { list of prepare-ID responses from other paxos-nodes
         */
        List<Tuple<Integer, Integer, Integer>> accept_request_responses =
                new ArrayList<Tuple<Integer, Integer, Integer>>();

        int temp_id = -1;

        for (Tuple<Integer, Integer, Integer> response : responses) {
            if (response != null && response.getClass().getName().equals("Tuple")) {
                if (response.y > temp_id) {
                    this.paxos_proposed_value = response.z;
                    temp_id = response.y;
                }
            }
        }

        if (this.paxos_proposed_value == -1) {
            List<Integer> keys = new ArrayList<Integer>(this.data_store.keySet());
            Collections.reverse(keys);

            this.paxos_proposed_value = keys.get(0) + 1;
        }

        for (Map.Entry<Integer, PaxosNode> pair : this.nodes.entrySet()) {
            int id = pair.getKey();
            PaxosNode node = pair.getValue();
            System.out.println(MessageFormat.format(
                    "node {0}, process {1}: sending ACCEPT-REQUEST(id {2}, " +
                            "value {3}) to node id {4}", this.id, Thread.currentThread().getId(),
                    this.paxos_proposed_id, this.paxos_proposed_value, node.id
            ));

            accept_request_responses.add(
                    node.accept_request(this.paxos_proposed_id, this.paxos_proposed_value)
            );
        }

        System.out.println(MessageFormat.format(
                "node {0}, process {1}: checking for majority for proposed-id {2}, " +
                        "accept-request-responses ", this.id, Thread.currentThread().getId(),
                this.paxos_proposed_id)+Tuple.viewTuple(accept_request_responses)
        );

        if (this.check_majority(accept_request_responses)) {
            System.out.println(MessageFormat.format(
                    "NODE {0} , PROCESS-ID {1}: CONSENSUS IS REACHED ON VALUE {2}",
                    this.id, Thread.currentThread().getId(), this.paxos_accepted_value
            ));

            HashMap<Integer, Integer> hmap = new HashMap<Integer, Integer>();
            List<Integer> keys = new ArrayList<Integer>(this.data_store.keySet());
            Collections.reverse(keys);
            hmap.put(0, this.data_store.get(keys.get(0)).get(0) + 2);
            this.data_store.put(this.paxos_accepted_value, hmap);

            System.out.println(MessageFormat.format(
                    "node-id {0}: current state of data-store: {1}",
                    this.id, this.data_store
            ));
        } else {
            this.send_prepares();
        }
    }

    private boolean check_majority(List<Tuple<Integer, Integer, Integer>> responses) {
        /*
         * param: responses list of prepare-ID/accept-request
         * responses from other paxos-nodes
         * return: True if majority, otherwise False
         */

        int response_count = responses.size() - Collections.frequency(responses, null);

        return (response_count >= this.majority);
    }

    public void run() {
        this.send_prepares();
    }
}


class Log {
    public static final Logger logger =
            Logger.getLogger(Double.toString(Thread.currentThread().getId())+": "+Log.class.getName());
}
