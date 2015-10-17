use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::io::prelude::*;
use std::u32;


pub const START_ID: &'static str = "_START";
pub const END_ID:   &'static str = "_END";


#[derive(Debug, Eq, PartialEq)]
pub struct Task {
    id: String,
    early_start: u32,
    early_finish: u32,
    late_start: u32,
    late_finish: u32,
    duration: u32,
    pred: HashSet<String>,
    succ: HashSet<String>,
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
            pred: HashSet::new(),
            succ: HashSet::new(),
        }
    }

    pub fn is_critical(&self) -> bool {
        self.early_start == self.late_start && self.early_finish == self.late_finish
    }
}


/// Parses a single line of input and adds it to the map.
///
/// An input line is expected to be of the form "A,d,B,C,...,N", where
///
/// - "A" is the label for the task to be added,
/// - "d" is some integer value for the task's duration, and
/// - "B,C,...,N" are labels of task (already in the map) on which "A" depends.
pub fn add_entry(line: &str, map: &mut HashMap<String, Task>) {
    let split: Vec<&str> = line.split(",").collect();
    assert!(split.len() >= 2, "Tasks must have both an ID and duration.");

    // step 0: id does not already exist in map
    assert!(!map.contains_key(split[0]), "Duplicate IDs are not allowed.");

    // step 1: make sure duration is an integer
    let duration = match split[1].parse::<u32>() {
        Ok(d)  => d,
        Err(_) => panic!("Only integers allowed for duration."),
    };

    let id = split[0].to_string();
    let mut task = Task::new(id.to_string(), duration);

    // step 2: for all dependencies, make sure they exist
    //         (i.e. the predecessors already exists)
    for d in split[2..].iter() {
        let mut dep_task = match map.get_mut(*d) {
            Some(v) => v,
            None => panic!("Invalid node found in dependency list."),
        };
        task.pred.insert(d.to_string());
        dep_task.succ.insert(id.to_string());
    }

    map.insert(id.to_string(), task);
}


pub fn add_start(map: &mut HashMap<String, Task>) {
    let mut start = Task::new(START_ID.to_string(), 0);
    for (id, task) in map.iter_mut() {
        if task.pred.is_empty() {
            task.pred.insert((&start.id).to_string());
            start.succ.insert((&id).to_string());
        }
    }
    map.insert(START_ID.to_string(), start);
}


pub fn add_end(map: &mut HashMap<String, Task>) {
    let mut end = Task::new(END_ID.to_string(), 0);
    for (id, task) in map.iter_mut() {
        if task.succ.is_empty() {
            task.succ.insert((&end.id).to_string());
            end.pred.insert((&id).to_string());
        }
    }
    map.insert(END_ID.to_string(), end);
}


pub fn propagate_forward(map: &mut HashMap<String,Task>) {
    let mut worklist: VecDeque<String> = VecDeque::new();
    worklist.push_back(String::from(START_ID));
    map.get_mut(START_ID).unwrap().early_start = 0;

    while !worklist.is_empty() {
        let cur_id = &worklist.pop_front().unwrap();
        let mut early_start;
        {
            let cur = map.get(cur_id).unwrap();

            // Add each successor to work list.
            for s in cur.succ.iter() {
                worklist.push_back(s.to_string());
            }

            // Find `early_start` for `cur` by finding the maximum `early_finish` of its preds.
            early_start = 0;
            for p_id in cur.pred.iter() {
                let p = map.get(p_id).unwrap();
                if p.early_finish > early_start {
                    early_start = p.early_finish;
                }
            }
        }

        // Get a mut reference to `cur` and update it.
        let mut cur = map.get_mut(cur_id).unwrap();
        cur.early_start = early_start;
        cur.early_finish = early_start + cur.duration;
    }
}


pub fn propagate_backward(map: &mut HashMap<String,Task>) {
    let mut worklist: VecDeque<String> = VecDeque::new();
    {
        // Initialize the end node, and add each predecessor of `end` to the work list.
        let end = map.get_mut(END_ID).unwrap();
        end.early_finish = end.early_start;
        end.late_start = end.early_start;
        end.late_finish = end.early_start;
        for p in end.pred.iter() {
            worklist.push_back(p.to_string());
        }
    }
    while !worklist.is_empty() {
        let cur_id = &worklist.pop_front().unwrap();
        let mut late_finish;
        {
            let cur = map.get(cur_id).unwrap();

            // Add each predecessor to work list.
            for s in cur.pred.iter() {
                worklist.push_back(s.to_string());
            }

            // Find `late_finish` for `cur` by finding the minimum `late_start` of its succs.
            late_finish = u32::MAX;
            for s_id in cur.succ.iter() {
                let s = map.get(s_id).unwrap();
                if s.late_start < late_finish {
                    late_finish = s.late_start;
                }
            }
        }

        // Get a mut reference to `cur` and update it.
        let mut cur = map.get_mut(cur_id).unwrap();
        cur.late_finish = late_finish;
        cur.late_start = late_finish - cur.duration;
    }
}


pub fn get_critical_tasks(map: &HashMap<String, Task>) -> Vec<String> {
    let mut vec = Vec::new();
    for (id, task) in map {
        if task.is_critical() {
            vec.push(id.to_string());
        }
    }
    vec
}


pub fn main() {
    let mut map: HashMap<String, Task> = HashMap::new();
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        add_entry(&line.unwrap(), &mut map);
    }
    add_start(&mut map);
    add_end(&mut map);
}


#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap};
    use std::u32;

    const MEDIUM_TEST_INPUT: [&'static str; 12] = [
        "A,2",
        "B,3",
        "C,2",
        "D,3,A",
        "E,2,B,C",
        "F,1,A,B",
        "G,4,A",
        "H,5,C",
        "I,3,D,F",
        "J,3,E,G",
        "K,2,I",
        "L,2,K"
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
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("A,2", &mut map);

        let e = map.get("A").unwrap();
        assert_eq!(e.id, "A".to_string());
        assert_eq!(e.early_start, 0);
        assert_eq!(e.early_finish, 0);
        assert_eq!(e.late_start, u32::MAX);
        assert_eq!(e.late_finish, u32::MAX);
        assert_eq!(e.duration, 2);
        assert_eq!(e.pred.len(), 0);
        assert_eq!(e.succ.len(), 0);
        assert_eq!(map.len(), 1);
    }

    #[test]
    #[should_panic]
    fn test_dup_node() {
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("A,2", &mut map);
        add_entry("A,2", &mut map);
    }

    #[test]
    #[should_panic]
    fn test_bad_dur_node() {
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("A,B", &mut map);
    }

    #[test]
    #[should_panic]
    fn test_no_node() {
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("", &mut map);
    }

    #[test]
    #[should_panic]
    fn test_no_dur() {
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("A", &mut map);
    }

    #[test]
    #[should_panic]
    fn test_should_exist() {
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("A,2,B", &mut map);
    }

    #[test]
    #[should_panic]
    fn test_self_dep() {
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("A,2,A", &mut map);
    }

    #[test]
    fn test_double_no_deps() {
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("A,2", &mut map);
        add_entry("B,1", &mut map);

        let e = map.get("A").unwrap();
        assert_eq!(e.id, "A".to_string());
        assert_eq!(e.early_start, 0);
        assert_eq!(e.early_finish, 0);
        assert_eq!(e.late_start, u32::MAX);
        assert_eq!(e.late_finish, u32::MAX);
        assert_eq!(e.duration, 2);
        assert_eq!(e.pred.len(), 0);
        assert_eq!(e.succ.len(), 0);

        let e = map.get("B").unwrap();
        assert_eq!(e.id, "B".to_string());
        assert_eq!(e.early_start, 0);
        assert_eq!(e.early_finish, 0);
        assert_eq!(e.late_start, u32::MAX);
        assert_eq!(e.late_finish, u32::MAX);
        assert_eq!(e.duration, 1);
        assert_eq!(e.pred.len(), 0);
        assert_eq!(e.succ.len(), 0);

        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_three_full_deps() {
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("A,2", &mut map);
        add_entry("B,1,A", &mut map);
        add_entry("C,3,A,B", &mut map);

        let e = map.get("A").unwrap();
        assert_eq!(e.id, "A".to_string());
        assert_eq!(e.early_start, 0);
        assert_eq!(e.early_finish, 0);
        assert_eq!(e.late_start, u32::MAX);
        assert_eq!(e.late_finish, u32::MAX);
        assert_eq!(e.duration, 2);
        assert_eq!(e.pred.len(), 0);
        assert_eq!(e.succ.len(), 2);
        assert!(e.succ.contains("B"));
        assert!(e.succ.contains("C"));

        let e = map.get("B").unwrap();
        assert_eq!(e.id, "B".to_string());
        assert_eq!(e.early_start, 0);
        assert_eq!(e.early_finish, 0);
        assert_eq!(e.late_start, u32::MAX);
        assert_eq!(e.late_finish, u32::MAX);
        assert_eq!(e.duration, 1);
        assert_eq!(e.pred.len(), 1);
        assert_eq!(e.succ.len(), 1);
        assert!(e.pred.contains("A"));
        assert!(e.succ.contains("C"));

        let e = map.get("C").unwrap();
        assert_eq!(e.id, "C".to_string());
        assert_eq!(e.early_start, 0);
        assert_eq!(e.early_finish, 0);
        assert_eq!(e.late_start, u32::MAX);
        assert_eq!(e.late_finish, u32::MAX);
        assert_eq!(e.duration, 3);
        assert_eq!(e.pred.len(), 2);
        assert_eq!(e.succ.len(), 0);
        assert!(e.pred.contains("A"));
        assert!(e.pred.contains("B"));

        assert_eq!(map.len(), 3);
    }

    #[test]
    fn test_add_start() {
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("A,2", &mut map);
        add_start(&mut map);
        let start = map.get(START_ID).unwrap();
        let task = map.get("A").unwrap();
        assert_eq!(start.succ.len(), 1);
        assert!(start.succ.contains("A"));
        assert_eq!(task.pred.len(), 1);
        assert!(task.pred.contains(START_ID));
    }

    #[test]
    fn test_add_end() {
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("A,2", &mut map);
        add_end(&mut map);
        let task = map.get("A").unwrap();
        let end = map.get(END_ID).unwrap();
        assert_eq!(task.succ.len(), 1);
        assert!(task.succ.contains(END_ID));
        assert_eq!(end.pred.len(), 1);
        assert!(end.pred.contains("A"));
    }

    #[test]
    fn test_medium_propagate_forward() {
        let mut map: HashMap<String, Task> = HashMap::new();
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
            assert!(task.early_start == expected, id);
        }

        for elem in MEDIUM_TEST_EXPECTED_EARLY_FINISH.iter() {
            let (id, expected) = *elem;
            let task = map.get(id).unwrap();
            assert!(task.early_finish == expected, id);
        }
    }

    #[test]
    fn test_medium_propagate_backward() {
        let mut map: HashMap<String, Task> = HashMap::new();
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
            assert!(task.late_start == expected, id);
        }

        for elem in MEDIUM_TEST_EXPECTED_LATE_FINISH.iter() {
            let (id, expected) = *elem;
            let task = map.get(id).unwrap();
            assert!(task.late_finish == expected, id);
        }
    }

    #[test]
    fn test_medium_get_critical_tasks() {
        let mut map: HashMap<String, Task> = HashMap::new();
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
