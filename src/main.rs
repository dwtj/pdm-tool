use std::collections::{HashMap, HashSet};
use std::io;
use std::io::prelude::*;


pub static START_ID: &'static str = "_START";
pub static END_ID:   &'static str = "_END";


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
            late_start: 0,
            late_finish: 0,
            duration: duration,
            pred: HashSet::new(),
            succ: HashSet::new(),
        }
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

    #[test]
    fn test_single_ok() {
        let mut map: HashMap<String, Task> = HashMap::new();
        add_entry("A,2", &mut map);
        let e = map.get("A").unwrap();
        assert_eq!(e.id, "A".to_string());
        assert_eq!(e.early_start, 0);
        assert_eq!(e.early_finish, 0);
        assert_eq!(e.late_start, 0);
        assert_eq!(e.late_finish, 0);
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
        assert_eq!(e.late_start, 0);
        assert_eq!(e.late_finish, 0);
        assert_eq!(e.duration, 2);
        assert_eq!(e.pred.len(), 0);
        assert_eq!(e.succ.len(), 0);

        let e = map.get("B").unwrap();
        assert_eq!(e.id, "B".to_string());
        assert_eq!(e.early_start, 0);
        assert_eq!(e.early_finish, 0);
        assert_eq!(e.late_start, 0);
        assert_eq!(e.late_finish, 0);
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
        assert_eq!(e.late_start, 0);
        assert_eq!(e.late_finish, 0);
        assert_eq!(e.duration, 2);
        assert_eq!(e.pred.len(), 0);
        assert_eq!(e.succ.len(), 2);
        assert!(e.succ.contains("B"));
        assert!(e.succ.contains("C"));

        let e = map.get("B").unwrap();
        assert_eq!(e.id, "B".to_string());
        assert_eq!(e.early_start, 0);
        assert_eq!(e.early_finish, 0);
        assert_eq!(e.late_start, 0);
        assert_eq!(e.late_finish, 0);
        assert_eq!(e.duration, 1);
        assert_eq!(e.pred.len(), 1);
        assert_eq!(e.succ.len(), 1);
        assert!(e.pred.contains("A"));
        assert!(e.succ.contains("C"));

        let e = map.get("C").unwrap();
        assert_eq!(e.id, "C".to_string());
        assert_eq!(e.early_start, 0);
        assert_eq!(e.early_finish, 0);
        assert_eq!(e.late_start, 0);
        assert_eq!(e.late_finish, 0);
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
}
