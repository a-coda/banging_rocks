extern crate clap;
extern crate crossbeam;
extern crate crossbeam_channel as channel;
extern crate itertools;
extern crate regex;
extern crate rocksdb;
extern crate walkdir;
 
use clap::{Arg, ArgMatches, App, AppSettings, SubCommand};
use itertools::Itertools;
use regex::Regex;
use rocksdb::{Options, DB, MergeOperands};
use std::boxed::Box;
use std::collections::HashSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use walkdir::WalkDir;
 
// Number of threads reading files
const THREAD_POOL_SIZE: usize = 10;
 
// Number of pending files to process before reading is blocked
const QUEUE_SIZE: usize = 100;
 
// Messages containing progress and filename info sent from walker to readers
struct Message(u32, std::sync::Arc<PathBuf>);
 
// Main entry point
fn main() {
    let matches = application().get_matches();
    match matches.subcommand() {
        ("index", Some(matches)) => {
            run_command(|index, args| index.create_index(args), matches, "BACKEND", "INDEX_DIRECTORY", "SOURCE_DIRECTORY");
        },
        ("search", Some(matches)) => {
            run_command(|index, args| index.search(args), matches, "BACKEND", "INDEX_DIRECTORY", "SEARCH_TERM");
        },
        ("statistics", Some(matches)) => {
            run_command(|index, args| index.statistics(args), matches, "BACKEND", "INDEX_DIRECTORY", "STATISTIC");
        },
        (_, _) => {
            panic!("cannot reach here");
        }
    }
}
 
// Command line argument parser
fn application() -> clap::App<'static, 'static> {
    App::new("Search")
        .version("1.0")
        .author("Jason Trenouth")
        .about("Builds a text index that allows searching")
        .setting(AppSettings::ArgRequiredElseHelp)
        .subcommand(SubCommand::with_name("index")
                    .about("indexes files")
                    .args(&common_args())
                    .arg(Arg::with_name("SOURCE_DIRECTORY")
                         .required(true)
                         .multiple(true)
                         .help("The directory to get the files from (recursivel)")))
        .subcommand(SubCommand::with_name("search")
                    .about("searches index")
                    .args(&common_args())
                    .arg(Arg::with_name("SEARCH_TERM")
                         .required(true)
                         .multiple(true)
                         .help("A search term")))
        .subcommand(SubCommand::with_name("statistics")
                    .about("print statistics about index")
                    .args(&common_args())
                    .arg(Arg::with_name("STATISTIC")
                         .required(false)
                         .multiple(true)
                         .help("A search term")))
}
 
// Some common command line arguments across subcommands
fn common_args<'a,'b>() -> [Arg<'a, 'b>; 2] {
    [ Arg::with_name("INDEX_DIRECTORY")
      .required(true)
      .help("The directory to put the index into"),
      Arg::with_name("BACKEND")
      .short("b")
      .long("backend")
      .takes_value(true)
      .default_value("rocksdb")
      .possible_values(&["rocksdb", "directory"])
      .help("The backend storage implementation to use")
      ]
}
 
// Glue between argument parsing and calling application methods
fn run_command<F>(operation: F, matches: &ArgMatches, backend_param: &str, directory_param: &str, rest_param: &str)
where F: Fn(Index, Vec<&str>) {
    let index = instantiate_index(matches, backend_param, directory_param);
    let args: Vec<_> = collect_rest_arguments(matches, rest_param);
    operation(index, args);
}
 
// Collects the rest of the arguments (eg search terms) after the primary ones
fn collect_rest_arguments<'a>(matches: &'a ArgMatches<'a>, rest_param: &str) -> Vec<&'a str> {
    if let Some(values) = matches.values_of(rest_param) {
        values.collect()
    }
    else
    {
        Vec::new()
    }
}
 
// Makes an index with the chosen backend type
fn instantiate_index(matches: &ArgMatches, backend_param: &str, directory_param: &str) -> Index {
    let directory = matches.value_of(directory_param).unwrap().to_string();
    match matches.value_of(backend_param).unwrap() {
        "rocksdb" => {
            Index::new(Box::new(RocksMultiMap::new(directory)))
        },
        "directory" => {
            Index::new(Box::new(DirectoryMultiMap::new(directory)))
        },
        _ => {
            panic!("cannot reach here");
        },
    }
}
 
// Abstract backend type which defines string to string-set multimap
trait PersistentMultiMap: std::marker::Sync
 {
    fn add(&self, key: &str, value: &str);
    fn get(&self, key: &str) -> HashSet<String>;
    fn set(&self, key: &str, HashSet<String>);
    fn keys(&self) -> Vec<String>;
}
 
// The high level index which just holds the chosen storage backend
struct Index {
    map: Box<PersistentMultiMap>
}
 
// The index type implements its functions against the abstracted storage
impl Index
{
 
    // Constructor
    fn new(map: Box<PersistentMultiMap>) -> Index {
        Index{ map }
    }
 
    // Finds the documents that match the terms (either intersecting or subtracting the sets)
    fn search(&self, terms: Vec<&str>) {
        let result = terms.iter()
            .map(|term| if term.starts_with("~") {(false, &term[1..])} else {(true, &term[..])})
            .map(|(includes, term)| (includes, self.map.get(term)))
            .fold1(|(_, mut r1), (includes, r2)| { r1.retain(|r| {if includes {r2.contains(r)} else {!r2.contains(r)}}); (true, r1)});
        if let Some((_, answers)) = result {
            for value in answers {
                println!("{}", value);
            }
        }
    }
 
    // Creates the index by launch a bunch of threads receiving files
    // and one thread finding and sending them
    fn create_index(&self, source_directories: Vec<&str>) {
        let (queue_s, queue_r) = channel::bounded(QUEUE_SIZE);
        crossbeam::scope(|scope| {
            let arc_queue_r = Arc::new(queue_r);
            for _ in 0..THREAD_POOL_SIZE {
                let arc_queue_r = Arc::clone(&arc_queue_r);
                scope.spawn(move || { self.receive_files(&arc_queue_r); });
            };
            scope.spawn(move || { self.send_files(source_directories, &queue_s); });
        });
    }
 
    // Loops grabbing files from the queue and processes them
    fn receive_files(&self, queue_r: &channel::Receiver<Option<Message>>) {
        loop {
            let message: Option<Message> = queue_r.recv().unwrap();
            if let Some(Message(progress, path)) = message { 
                if progress % 100 == 0 {
                    println!("progress = {}", progress);
                }
                self.visit(&path);
            }
            else {
                break;
            }
        }
    }
 
    // Walks the file system send files found to the queue
    fn send_files(&self, source_directories: Vec<&str>, queue_s: &channel::Sender<Option<Message>>) {
        self.walk_files(source_directories, &|progress, path| queue_s.send(Some(Message(progress, path))));
        for _ in 0..THREAD_POOL_SIZE {
            queue_s.send(None);
        }
    }
 
    // Walks the file system invoking the callback on each file found
    fn walk_files(&self, directories: Vec<&str>, callback: &Fn(u32, Arc<PathBuf>)) {
        let mut progress = 0;
        for dir in directories {
            for entry in WalkDir::new(dir) {
                if let Ok(dir_entry) = entry {
                    if let Ok(metadata) = dir_entry.metadata() {
                        if metadata.is_file() {
                            progress += 1;
                            let arc_path = Arc::new(dir_entry.path().to_path_buf());
                            callback(progress, Arc::clone(&arc_path));
                        }
                    }
                }
            }
        }
    }
 
    // Reads each file and adds its filename to the map entry for each word found
    fn visit(&self, path: &Path) {
        if let Some(source_word_file) = path.to_str() {
            let mut source_file = File::open(path).expect(&format!("source file does not exist: {}", source_word_file));
            let mut source_text = String::new();
            if source_file.read_to_string(&mut source_text).is_ok() {
                let re = Regex::new(r"\W+").expect("illegal regex");
                let source_words: HashSet<&str> = re.split(&source_text).collect();
                for source_word in source_words {
                    if ! source_word.is_empty() {
                        self.map.add(source_word, source_word_file);
                    }
                }
            }
        }
    }
 
    // Prints the number of unique words found
    fn statistics(&self, _statistics: Vec<&str>) {
        println!("number of keys: {}", self.map.keys().len())
    }
}
 
// Directory-based implementation refers to directory name
struct DirectoryMultiMap {
    directory: PathBuf
}
 
// Directory-only functions
impl DirectoryMultiMap {
 
    // Constructor makes the directory for the index if it doesn't exist
    fn new(directory: String) -> DirectoryMultiMap {
        fs::create_dir_all(&directory).expect(&format!("directory cannot be created: {}", directory)); 
        DirectoryMultiMap{ directory: PathBuf::from(directory) }
    }
 
    // Returns the filename for the index entry in the directory
    fn get_path(&self, key: &str) -> PathBuf {
        self.directory.join(key.to_lowercase())
    }
}
 
// Directory-based implementation of the trait
impl PersistentMultiMap for DirectoryMultiMap {
 
    // Appends the new filename to the end of the directory entry representing the word
    fn add(&self, key: &str, value: &str) {
        match OpenOptions::new().create(true).append(true).open(&self.get_path(key)) {
            Ok(mut key_file) => {
                if let Err(error) = write!(key_file, "{}\n", value) {
                    println!("Warning: could not write to: {}: {}", key, error);
                }
            }
            Err(error) => {
                println!("Warning: could not append to: {}: {}", key, error);
            }
        }
    }
 
    // Returns the directory entry (plain text list of filenames) as a set
    fn get(&self, key: &str) -> HashSet<String> {
        let mut values = HashSet::new();
        let key_path = self.get_path(&key);
        if key_path.exists() {
            let reader = BufReader::new(File::open(&key_path).expect(&format!("key file does not exist: {}", key)));
            for line in reader.lines() {
                if let Ok(value) = line {
                    values.insert(value.to_string());
                }
            }
        }
        values
    }
 
    fn set(&self, _key: &str, _values: HashSet<String>) {
        panic!("not implemented");
    }
 
    // Enumerates all the files in the directory
    fn keys(&self) -> Vec<String> {
        fs::read_dir(&self.directory).expect(&format!("directory cannot be read: {}", self.directory.to_str().unwrap()))
            .map(|entry| entry.unwrap().path().to_str().unwrap().to_string())
            .collect()
    }
}
 
// RocksDB implementation refers to database instance
struct RocksMultiMap {
    db: DB
}
 
// Merge callback function joins current and new values together with newline as delimiter
fn concatenate(_key: &[u8], value: Option<&[u8]>, operands: &mut MergeOperands) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::new();
    if let Some(v) = value {
        result.extend(v);
    }
    for op in operands {
        result.extend("\n".as_bytes());
        result.extend(op);
    }
    Some(result)
}
 
// RocksDB-only functions
impl RocksMultiMap {
 
    // Constructor opens the database
    fn new(directory: String) -> RocksMultiMap {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator("concatenate", concatenate, None);
        match DB::open(&opts, directory) {
            Ok(db) => { RocksMultiMap{ db } },
            Err(e) => { panic!("could not open database: {}", e) }
        }
    }
 
    // Turns a set of filenames into a sequence of bytes
    fn serialize(&self, values: HashSet<String>) -> Vec<u8> {
        values.iter().join("\n").as_bytes().to_vec()
    }
 
    // Turns a sequence of bytes into a set of filenames
    fn deserialize(&self, data: Vec<u8>) -> HashSet<String> {
        let mut result = HashSet::new();
        let strdata = String::from_utf8(data).unwrap();
        for value in strdata.split("\n") {
            result.insert(value.to_string());
        }
        result
    }
 
    // Returns a standardised key out of a word
    fn enkey(&self, key: &str) -> Vec<u8> {
        key.to_lowercase().as_bytes().to_vec()
    }
}
 
// RocksDB implementation of the trait
impl PersistentMultiMap for RocksMultiMap {
 
    // Ask the db to merge the filename for us rather than do read-modify-write on the set
    fn add(&self, key: &str, value: &str) {
        if let Err(e) = self.db.merge(&self.enkey(key), value.as_bytes()) {
            panic!("error merging value into database {}", e);
        }
    }
 
    // Look up bytes in the store and turn into a set of filenames
    fn get(&self, key: &str) -> HashSet<String> {
        match self.db.get(&self.enkey(key)) {
            Ok(Some(data)) => {
                self.deserialize(data.to_vec())
            },
            Ok(None) => {
                HashSet::new()
            },
            Err(e) => panic!("error accessing database: {}", e),
        }
    }
 
    // Store a set of filenames in the store
    fn set(&self, key: &str, values: HashSet<String>) {
        if let Err(e) = self.db.put(&self.enkey(key), &self.serialize(values)) {
            panic!("error putting value into database {}", e);
        }
    }
 
    // Enumerate all the keys in the store
    fn keys(&self) -> Vec<String> {
        let mut result = Vec::new();
        let mut iter = self.db.raw_iterator();
        iter.seek_to_first();
        while iter.valid() {
            result.push(String::from_utf8(iter.key().unwrap()).unwrap());
            iter.next();
        }
        result
    }
}
