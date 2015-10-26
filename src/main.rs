use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::path::Path;
use std::rc::{Rc};
use std::u32;


pub const START_ID: &'static str = "START";
pub const END_ID:   &'static str = "END";

#[derive(Debug, Eq)]
pub struct Task {
    id: String,
    early_start: u32,
    early_finish: u32,
    late_start: u32,
    late_finish: u32,
    duration: u32,
    pred: Vec<RCTask>,
    succ: Vec<RCTask>,
}

pub type RCTaskMap = HashMap<String, RCTask>;
pub type RCTask = Rc<RefCell<Task>>;

impl PartialEq for Task {
    fn eq(&self, other: &Task) -> bool {
        self.id == other.id
    }
}

impl Task {
    pub fn new(id: String, duration: u32) -> Task {
        Task {
            id: id,
            early_start: 0,
            early_finish: 0,
            late_start: u32::MAX,
            late_finish: u32::MAX,
            duration: duration,
            pred: Vec::new(),
            succ: Vec::new(),
        }
    }

    pub fn rc_new(id: String, duration: u32) -> RCTask {
        Rc::new(RefCell::new(Task::new(id.to_string(), duration)))
    }

    pub fn is_critical(&self) -> bool {
        self.early_start == self.late_start &&
        self.early_finish == self.late_finish
    }

    pub fn succ_ids(&self) -> Vec<String> {
        self.succ.iter().map(|i| i.borrow().id.to_string())
                        .collect::<Vec<String>>()
    }

    pub fn pred_ids(&self) -> Vec<String> {
        self.pred.iter().map(|i| i.borrow().id.to_string())
                        .collect::<Vec<String>>()
    }
}

/// Parses a single line of input and adds it to the map.
///
/// An input line is expected to be of the form "A,d,B,C,...,N", where
///
/// - "A" is the label for the task to be added,
/// - "d" is some integer value for the task's duration, and
/// - "B,C,...,N" are labels of task (already in the map) on which "A" depends.
pub fn add_entry(line: &str, map: &mut RCTaskMap) {
    let split: Vec<&str> = line.split(" ").collect();
    assert!(split.len() == 2 || split.len() == 3,
            "Tasks must have both an ID and duration and at most one \
             dependency list.");

    // step 0: id does not already exist in map
    assert!(!map.contains_key(split[0]), "Duplicate IDs are not allowed.");

    // step 1: make sure duration is an integer
    let duration = match split[1].parse::<u32>() {
        Ok(d)  => d,
        Err(_) => panic!("Only integers allowed for duration."),
    };

    let id = split[0].to_string();
    let task = Task::rc_new(id.to_string(), duration);

    // if we have a dependency list, parse it and add to map
    if split.len() == 3 {
        // collect dependencies and remove redundancies
        let deps: HashSet<&str> = split[2].split(",").collect(); 
        // step 2: for all dependencies, make sure they exist
        //         (i.e. the predecessors already exist)
        for d in deps.iter() {
            let dep_task = match map.get_mut(*d) {
                Some(v) => v,
                None => panic!("Invalid task in dependency list."),
            };
            task.borrow_mut().pred.push(dep_task.clone());
            dep_task.borrow_mut().succ.push(task.clone());
        }
    }
    map.insert(id.to_string(), task);
}

pub fn add_start(map: &mut RCTaskMap) {
    let start = Task::rc_new(START_ID.to_string(), 0);
    for task in map.values() {
        if task.borrow().pred.is_empty() {
            task.borrow_mut().pred.push(start.clone());
            start.borrow_mut().succ.push(task.clone());
        }
    }
    map.insert(START_ID.to_string(), start);
}

pub fn add_end(map: &mut RCTaskMap) {
    let end = Task::rc_new(END_ID.to_string(), 0);
    for task in map.values() {
        if task.borrow().succ.is_empty() {
            task.borrow_mut().succ.push(end.clone());
            end.borrow_mut().pred.push(task.clone());
        }
    }
    map.insert(END_ID.to_string(), end);
}


pub fn propagate_forward(map: &mut RCTaskMap) {
    let mut worklist: VecDeque<String> = VecDeque::new();
    worklist.push_back(START_ID.to_string());

    while !worklist.is_empty() {
        let cur = map.get_mut(&worklist.pop_front().unwrap()).unwrap();
        // Add each successor to work list.
        worklist.extend(cur.borrow().succ_ids());
        // Find the max early_finish of cur's predecessors
        let early_start = match cur.borrow().pred.iter().map(
                                |x| x.borrow().early_finish).max() {
            Some(v) => v,
            None    => 0,
        };
        cur.borrow_mut().early_start = early_start;
        let new_dur = early_start + cur.borrow().duration;
        cur.borrow_mut().early_finish = new_dur;
    }
}

pub fn propagate_backward(map: &mut RCTaskMap) {
    let mut worklist: VecDeque<String> = VecDeque::new();
    {
        let end = map.get_mut(END_ID).unwrap();
        let mut t = end.borrow().early_start; end.borrow_mut().early_finish = t;
        t = end.borrow().early_start; end.borrow_mut().late_start = t;
        t = end.borrow().early_start; end.borrow_mut().late_finish = t;
        worklist.extend(end.borrow().pred_ids());
    }

    while !worklist.is_empty() {
        let cur = map.get_mut(&worklist.pop_front().unwrap()).unwrap();
        // Add each predecessor to work list.
        worklist.extend(cur.borrow().pred_ids());
        // Find the min late start of cur's successors
        let late_finish = match cur.borrow().succ.iter().map(
                                |x| x.borrow().late_start).min() {
            Some(v) => v,
            None    => u32::MAX,
        };
        cur.borrow_mut().late_finish = late_finish;
        let new_ls = late_finish - cur.borrow().duration;
        cur.borrow_mut().late_start = new_ls;
    }
}

pub fn get_critical_tasks(map: &RCTaskMap) -> Vec<String> {
    let mut ct: Vec<&RCTask> = map.values()
                              .filter(|i| i.borrow().is_critical())
                              .collect();
    ct.sort_by(|a,b| {
                        if a.borrow().early_start < b.borrow().early_start {
                            Ordering::Less
                        }
                        else if a.borrow().early_start > b.borrow().early_start{
                            Ordering::Greater
                        }
                        else {
                            Ordering::Equal
                        }
    });
    ct.iter().map(|i| i.borrow().id.to_string()).collect()
}

// print the output for the assignment. Format is:
//    - Node,
// (ES, EF, LS, LF)
// Critical path: You need to compute the critical paths and display them
pub fn display(m: &RCTaskMap) {
    
    println!("Node,ES,EF,LS,LF");
    for node in m.values() {
        println!("{:?},{:?},{:?},{:?},{:?}", node.borrow().id,
                                             node.borrow().early_start,
                                             node.borrow().early_finish,
                                             node.borrow().late_start,
                                             node.borrow().late_finish);
    }
    print!("\nCritical Path: ");
    for t in get_critical_tasks(m) {
        print!("{:?},", t);
    }
    println!("");
}

pub fn main() {

    let args: Vec<String> = env::args().collect();
    assert!(args.len() == 2, "Usage: ./pdm-tool filename");
    let mut map: RCTaskMap = HashMap::new();

    let file = File::open(Path::new(&args[1])).unwrap();
    let reader = BufReader::new(file);
    for line in reader.lines() {
        add_entry(&line.unwrap(), &mut map);
    }

    add_start(&mut map);
    add_end(&mut map);
    propagate_forward(&mut map);
    propagate_backward(&mut map);
    display(&map);
}


#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap};
    use std::u32;

    const MEDIUM_TEST_INPUT: [&'static str; 12] = [
        "A 2",
        "B 3",
        "C 2",
        "D 3 A",
        "E 2 B,C",
        "F 1 A,B",
        "G 4 A",
        "H 5 C",
        "I 3 D,F",
        "J 3 E,G",
        "K 2 I",
        "L 2 K"
    ];

    const MEDIUM_TEST_EXPECTED_EARLY_START: [(&'static str, u32); 14] = [
        (START_ID, 0),
        ("A", 0),
        ("B", 0),
        ("C", 0),
        ("D", 2),
        ("E", 3),
        ("F", 3),
        ("G", 2),
        ("H", 2),
        ("I", 5),
        ("J", 6),
        ("K", 8),
        ("L", 10),
        (END_ID, 12),
    ];

    const MEDIUM_TEST_EXPECTED_EARLY_FINISH: [(&'static str, u32); 14] = [
        (START_ID, 0),
        ("A", 2),
        ("B", 3),
        ("C", 2),
        ("D", 5),
        ("E", 5),
        ("F", 4),
        ("G", 6),
        ("H", 7),
        ("I", 8),
        ("J", 9),
        ("K", 10),
        ("L", 12),
        (END_ID, 12),
    ];

    const MEDIUM_TEST_EXPECTED_LATE_FINISH: [(&'static str, u32); 14] = [
        (START_ID, 0),
        ("A", 2),
        ("B", 4),
        ("C", 7),
        ("D", 5),
        ("E", 9),
        ("F", 5),
        ("G", 9),
        ("H", 12),
        ("I", 8),
        ("J", 12),
        ("K", 10),
        ("L", 12),
        (END_ID, 12),
    ];

    const MEDIUM_TEST_EXPECTED_LATE_START: [(&'static str, u32); 14] = [
        (START_ID, 0),
        ("A", 0),
        ("B", 1),
        ("C", 5),
        ("D", 2),
        ("E", 7),
        ("F", 4),
        ("G", 5),
        ("H", 7),
        ("I", 5),
        ("J", 9),
        ("K", 8),
        ("L", 10),
        (END_ID, 12),
    ];

    const MEDIUM_TEST_EXPECTED_CRITICAL_TASKS: [&'static str; 7] = [
        START_ID,
        "A",
        "D",
        "I",
        "K",
        "L",
        END_ID,
    ];

    #[test]
    fn test_single_ok() {
        let mut map: RCTaskMap = HashMap::new();
        add_entry("A 2", &mut map);

        let e = map.get("A").unwrap();
        assert_eq!(e.borrow().id, "A".to_string());
        assert_eq!(e.borrow().early_start, 0);
        assert_eq!(e.borrow().early_finish, 0);
        assert_eq!(e.borrow().late_start, u32::MAX);
        assert_eq!(e.borrow().late_finish, u32::MAX);
        assert_eq!(e.borrow().duration, 2);
        assert_eq!(e.borrow().pred.len(), 0);
        assert_eq!(e.borrow().succ.len(), 0);
        assert_eq!(map.len(), 1);
    }

    #[test]
    #[should_panic]
    fn test_dup_node() {
        let mut map: RCTaskMap = HashMap::new();
        add_entry("A 2", &mut map);
        add_entry("A 2", &mut map);
    }

    #[test]
    #[should_panic]
    fn test_bad_dur_node() {
        let mut map: RCTaskMap = HashMap::new();
        add_entry("A B", &mut map);
    }

    #[test]
    #[should_panic]
    fn test_no_node() {
        let mut map: RCTaskMap = HashMap::new();
        add_entry("", &mut map);
    }

    #[test]
    #[should_panic]
    fn test_no_dur() {
        let mut map: RCTaskMap = HashMap::new();
        add_entry("A", &mut map);
    }

    #[test]
    #[should_panic]
    fn test_should_exist() {
        let mut map: RCTaskMap = HashMap::new();
        add_entry("A 2 B", &mut map);
    }

    #[test]
    #[should_panic]
    fn test_self_dep() {
        let mut map: RCTaskMap = HashMap::new();
        add_entry("A 2 A", &mut map);
    }

    #[test]
    fn test_double_no_deps() {
        let mut map: RCTaskMap = HashMap::new();
        add_entry("A 2", &mut map);
        add_entry("B 1", &mut map);

        let e = map.get("A").unwrap();
        assert_eq!(e.borrow().id, "A".to_string());
        assert_eq!(e.borrow().early_start, 0);
        assert_eq!(e.borrow().early_finish, 0);
        assert_eq!(e.borrow().late_start, u32::MAX);
        assert_eq!(e.borrow().late_finish, u32::MAX);
        assert_eq!(e.borrow().duration, 2);
        assert_eq!(e.borrow().pred.len(), 0);
        assert_eq!(e.borrow().succ.len(), 0);

        let e = map.get("B").unwrap();
        assert_eq!(e.borrow().id, "B".to_string());
        assert_eq!(e.borrow().early_start, 0);
        assert_eq!(e.borrow().early_finish, 0);
        assert_eq!(e.borrow().late_start, u32::MAX);
        assert_eq!(e.borrow().late_finish, u32::MAX);
        assert_eq!(e.borrow().duration, 1);
        assert_eq!(e.borrow().pred.len(), 0);
        assert_eq!(e.borrow().succ.len(), 0);

        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_three_full_deps() {
        let mut map: RCTaskMap = HashMap::new();
        add_entry("A 2", &mut map);
        add_entry("B 1 A", &mut map);
        add_entry("C 3 A,B", &mut map);

        let e = map.get("A").unwrap();
        assert_eq!(e.borrow().id, "A".to_string());
        assert_eq!(e.borrow().early_start, 0);
        assert_eq!(e.borrow().early_finish, 0);
        assert_eq!(e.borrow().late_start, u32::MAX);
        assert_eq!(e.borrow().late_finish, u32::MAX);
        assert_eq!(e.borrow().duration, 2);
        assert_eq!(e.borrow().pred.len(), 0);
        assert_eq!(e.borrow().succ.len(), 2);
        let mut t = e.borrow().succ_ids();
        t.sort();
        assert_eq!(t, ["B", "C"]);

        let e = map.get("B").unwrap();
        assert_eq!(e.borrow().id, "B".to_string());
        assert_eq!(e.borrow().early_start, 0);
        assert_eq!(e.borrow().early_finish, 0);
        assert_eq!(e.borrow().late_start, u32::MAX);
        assert_eq!(e.borrow().late_finish, u32::MAX);
        assert_eq!(e.borrow().duration, 1);
        assert_eq!(e.borrow().pred.len(), 1);
        assert_eq!(e.borrow().succ.len(), 1);
        assert_eq!(e.borrow().pred_ids(), ["A"]);
        assert_eq!(e.borrow().succ_ids(), ["C"]);

        let e = map.get("C").unwrap();
        assert_eq!(e.borrow().id, "C".to_string());
        assert_eq!(e.borrow().early_start, 0);
        assert_eq!(e.borrow().early_finish, 0);
        assert_eq!(e.borrow().late_start, u32::MAX);
        assert_eq!(e.borrow().late_finish, u32::MAX);
        assert_eq!(e.borrow().duration, 3);
        assert_eq!(e.borrow().pred.len(), 2);
        assert_eq!(e.borrow().succ.len(), 0);
        let mut t = e.borrow().pred_ids();
        t.sort();
        assert_eq!(t, ["A", "B"]);

        assert_eq!(map.len(), 3);
    }

    #[test]
    fn test_add_start() {
        let mut map: RCTaskMap = HashMap::new();
        add_entry("A 2", &mut map);
        add_start(&mut map);
        let start = map.get(START_ID).unwrap();
        let task = map.get("A").unwrap();
        assert_eq!(start.borrow().succ.len(), 1);
        assert_eq!(start.borrow().succ_ids(), ["A"]);
        assert_eq!(task.borrow().pred.len(), 1);
        assert_eq!(task.borrow().pred_ids(), [START_ID]);
    }

    #[test]
    fn test_add_end() {
        let mut map: RCTaskMap = HashMap::new();
        add_entry("A 2", &mut map);
        add_end(&mut map);
        let task = map.get("A").unwrap();
        let end = map.get(END_ID).unwrap();
        assert_eq!(task.borrow().succ.len(), 1);
        assert_eq!(task.borrow().succ_ids(), [END_ID]);
        assert_eq!(end.borrow().pred.len(), 1);
        assert_eq!(end.borrow().pred_ids(), ["A"]);
    }

    #[test]
    fn test_medium_propagate_forward() {
        let mut map: RCTaskMap = HashMap::new();
        for line in MEDIUM_TEST_INPUT.iter() {
            add_entry(line, &mut map);
        }
        add_start(&mut map);
        add_end(&mut map);
        assert_eq!(map.len(), MEDIUM_TEST_INPUT.len() + 2);

        propagate_forward(&mut map);

        for elem in MEDIUM_TEST_EXPECTED_EARLY_START.iter() {
            let (id, expected) = *elem;
            let task = map.get(id).unwrap();
            assert!(task.borrow().early_start == expected, id);
        }

        for elem in MEDIUM_TEST_EXPECTED_EARLY_FINISH.iter() {
            let (id, expected) = *elem;
            let task = map.get(id).unwrap();
            assert!(task.borrow().early_finish == expected, id);
        }
    }

    #[test]
    fn test_medium_propagate_backward() {
        let mut map: RCTaskMap = HashMap::new();
        for line in MEDIUM_TEST_INPUT.iter() {
            add_entry(line, &mut map);
        }
        add_start(&mut map);
        add_end(&mut map);
        assert_eq!(map.len(), MEDIUM_TEST_INPUT.len() + 2);

        propagate_forward(&mut map);
        propagate_backward(&mut map);

        for elem in MEDIUM_TEST_EXPECTED_LATE_START.iter() {
            let (id, expected) = *elem;
            let task = map.get(id).unwrap();
            println!("\n\nexpected: {:?}, actual: {:?}\n", expected, task.borrow().late_start);
            assert!(task.borrow().late_start == expected, id);
        }

        for elem in MEDIUM_TEST_EXPECTED_LATE_FINISH.iter() {
            let (id, expected) = *elem;
            let task = map.get(id).unwrap();
            assert!(task.borrow().late_finish == expected, id);
        }
    }

    #[test]
    fn test_medium_get_critical_tasks() {
        let mut map: RCTaskMap = HashMap::new();
        for line in MEDIUM_TEST_INPUT.iter() {
            add_entry(line, &mut map);
        }
        add_start(&mut map);
        add_end(&mut map);
        assert_eq!(map.len(), MEDIUM_TEST_INPUT.len() + 2);

        propagate_forward(&mut map);
        propagate_backward(&mut map);
        let actual = get_critical_tasks(&map);
        assert_eq!(actual.len(), MEDIUM_TEST_EXPECTED_CRITICAL_TASKS.len());
        for expected_id in MEDIUM_TEST_EXPECTED_CRITICAL_TASKS.iter() {
            assert!(includes_str(&actual, expected_id), "Didn't find expected {:?} in actual {:?}",
                                                        expected_id, actual);
        }
    }

    fn includes_str(vec: &Vec<String>, target: &str) -> bool {
        for elem in vec.iter() {
            if *elem == *target {
                return true;
            }
        }
        false
    }
}
