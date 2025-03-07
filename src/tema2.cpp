#include <mpi.h>
#include <pthread.h>
#include <bits/stdc++.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ERR(cond, msg) \
    if (cond) { \
        puts(msg); \
        exit(-1); \
    }

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

// Messages
#define ACK_MSG "Y"
#define NACK_MSG "N"
#define RESPONSE_LEN 2

// Message tags
enum Tags {
    INIT_TAG,
    START_TAG,
    END_TAG,
    SWARM_REQUEST_TAG,
    TRACKER_FILE_TAG,
    PEER_SEGMENT_REQUEST,
    PEER_SEGMENT_SEND
};

/*
 * peer_filecount = number of files owned by peer
 * peer_filenames = names of files owned by peer
 * peer_chunk_count = chunk count of owned files
 * peer_files = actual content of owned files
 * 
 * desired_filecount = number of files that the peer wants to download
 * desired_filenames = names of files that the peer wants
*/
struct Peer_data {
    int peer_filecount;
    char peer_filenames[MAX_FILES][MAX_FILENAME + 1];
    int peer_chunk_count[MAX_FILES];
    char peer_files[MAX_FILES][MAX_CHUNKS][HASH_SIZE + 1];

    int desired_filecount;
    char desired_filenames[MAX_FILES][MAX_FILENAME + 1];
} local_peer_data;

struct Thread_arg {
    int rank;
    int numtasks;
};

// MPI datatype for the peer data
MPI_Datatype MPI_peer_struct;

// Mutex to sync access to segment count of files
pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * file = name of the desired file
 * len = number of segments of the file
 * swarm = array of the peers in the swarm
 * peer_count = number of peers int the swarm
*/
void get_file_swarm(char* file, int &len, int* &swarm, int &peer_count)
{
    int msg_len = strlen(file) + 1;

    // Send filename and filename length to tracker
    MPI_Ssend(&msg_len, 1, MPI_INT, TRACKER_RANK, SWARM_REQUEST_TAG, MPI_COMM_WORLD);
    MPI_Ssend(file, msg_len, MPI_CHAR, TRACKER_RANK, SWARM_REQUEST_TAG, MPI_COMM_WORLD);

    // Receive swarm, file length and the number of peers in the swarm from tracker 
    MPI_Recv(&peer_count, 1, MPI_INT, TRACKER_RANK, SWARM_REQUEST_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&len, 1, MPI_INT, TRACKER_RANK, SWARM_REQUEST_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    swarm = (int*) realloc(swarm, peer_count * sizeof(int));
    MPI_Recv(swarm, peer_count, MPI_INT, TRACKER_RANK, SWARM_REQUEST_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

void get_file_from_tracker(char* file, int len, int rank)
{
    // Send desired file to tracker
    int filename_len = strlen(file) + 1;
    MPI_Ssend(&filename_len, 1, MPI_INT, TRACKER_RANK, TRACKER_FILE_TAG, MPI_COMM_WORLD);
    MPI_Ssend(file, filename_len, MPI_CHAR, TRACKER_RANK, TRACKER_FILE_TAG, MPI_COMM_WORLD);

    std::vector<std::string> file_contents;

    for (int i = 0; i < len; i++) {
        char hash[HASH_SIZE + 1];

        MPI_Recv(hash, HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, TRACKER_FILE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        file_contents.push_back(hash);
    }

    char out_filename[MAX_FILENAME + 33] = {0};

    sprintf(out_filename, "client%d_%s", rank, file);

    FILE *out_file = fopen(out_filename, "w");

    for (int i = 0; i < len; i++) {
        fprintf(out_file, "%s\n", file_contents[i].c_str());
    }

    fclose(out_file);
}

void *download_thread_func(void *arg)
{
    Thread_arg args = *(Thread_arg*) arg;
    int rank = args.rank;
    int numtasks = args.numtasks;


    // Order peers by load
    std::multimap<unsigned int, int> peer_loads;

    int segment_count, peer_count;
    int *loads = (int*) calloc(numtasks, sizeof(int));
    int *swarm;
    char *file;

    // Iterate through all the files that the peer wants to download
    for (int i = 0; i < local_peer_data.desired_filecount; i++) {
        // Set loads to 0 for current file
        memset(loads, 0, numtasks * sizeof(int));

        file = local_peer_data.desired_filenames[i];

        get_file_swarm(file, segment_count, swarm, peer_count);

        // Add file to owned files
        pthread_mutex_lock(&file_mutex);

        // Add the file to peer swarm with owned segments = 0
        strcpy(local_peer_data.peer_filenames[local_peer_data.peer_filecount], file);
        local_peer_data.peer_chunk_count[local_peer_data.peer_filecount] = 0;
        local_peer_data.peer_filecount++; 

        pthread_mutex_unlock(&file_mutex);

        for (int seg = 0; seg < segment_count; seg++) {

            // Get swarm every 10 segments
            if (seg % 10 == 0) {
                peer_loads.clear();

                get_file_swarm(file, segment_count, swarm, peer_count);

                // Update peer loads
                for (int i = 0; i < peer_count; i++) {
                    if (swarm[i] == rank)
                        continue;
                    
                    peer_loads.insert({loads[swarm[i]], swarm[i]});
                }
            }

            // Download segment
            for (auto& pair : peer_loads) {
                int peer = pair.second;

                int filename_len = strlen(file) + 1;
                // Send filename length, filename and desired segment
                MPI_Ssend(&filename_len, 1, MPI_INT, peer, PEER_SEGMENT_REQUEST, MPI_COMM_WORLD);
                MPI_Ssend(file, filename_len, MPI_CHAR, peer, PEER_SEGMENT_REQUEST, MPI_COMM_WORLD);
                MPI_Ssend(&seg, 1, MPI_INT, peer, PEER_SEGMENT_REQUEST, MPI_COMM_WORLD);

                char response[8] = {0};
                MPI_Recv(response, RESPONSE_LEN, MPI_CHAR, peer, PEER_SEGMENT_SEND, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // Break loop upon getting ACK
                if (!strcmp(response, ACK_MSG)) {
                    // Increase the load of the peer which sent ACK
                    loads[peer]++;

                    // Update owned segment count
                    pthread_mutex_lock(&file_mutex);
                    local_peer_data.peer_chunk_count[local_peer_data.peer_filecount - 1]++;
                    pthread_mutex_unlock(&file_mutex);

                    break;
                }
            }
        }

        // After all segment ACKs were received get the hashes from the tracker
        get_file_from_tracker(file, segment_count, rank);
    }

    free(swarm);
    free(loads);
    
    // Send msg to tracker that peer has finished downloading all desired files
    MPI_Ssend(nullptr, 0, MPI_CHAR, TRACKER_RANK, END_TAG, MPI_COMM_WORLD);

    return NULL;
}

void *upload_thread_func(void *arg)
{
    MPI_Status status;

    int source, tag, done = 0, filename_len, segment, found;
    unsigned int load = 0;
    void *msg_buffer;

    // Upload logic loop, will handle different types of messages from peers/tracker
    while(!done) {
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        source = status.MPI_SOURCE;
        tag = status.MPI_TAG;

        switch (tag) {
            // A peer wants a segment for a file we own
            case PEER_SEGMENT_REQUEST:
                // Receive filename length, filename and desired segment
                MPI_Recv(&filename_len, 1, MPI_INT, source, PEER_SEGMENT_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                msg_buffer = malloc(filename_len);
                MPI_Recv(msg_buffer, filename_len, MPI_CHAR, source, PEER_SEGMENT_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Recv(&segment, 1, MPI_INT, source, PEER_SEGMENT_REQUEST, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                found = 0;

                // Check if we have the segment, i.e. chunk_count[file] > segment
                for (int i = 0; i < local_peer_data.peer_filecount; i++) {
                    if (!strcmp((char*)msg_buffer, local_peer_data.peer_filenames[i])) {
                        
                        pthread_mutex_lock(&file_mutex);
                        if (local_peer_data.peer_chunk_count[i] > segment) {
                            found = 1;
                        }
                        pthread_mutex_unlock(&file_mutex);

                        break;
                    }
                }

                if (found) {
                    load++;
                    // Segment found, send ACK message
                    MPI_Ssend(ACK_MSG, RESPONSE_LEN, MPI_CHAR, source, PEER_SEGMENT_SEND, MPI_COMM_WORLD);
                } else {
                    // Segment not found, send NACK message
                    MPI_Ssend(NACK_MSG, RESPONSE_LEN, MPI_CHAR, source, PEER_SEGMENT_SEND, MPI_COMM_WORLD);
                }

                free(msg_buffer);
                break;

            // End message from TRACKER, break logic loop and exit
            case END_TAG:
                if (source == TRACKER_RANK) {
                    MPI_Recv(nullptr, 0, MPI_CHAR, TRACKER_RANK, END_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    done = 1;
                }
                break;
                
            default:
                break;
        }
    }

    return NULL;
}

// Read input data for each peer
void read_peer_input(int rank, Peer_data *data)
{
    int rc = 0;

    char input_filename[MAX_FILENAME + 1] = {0};
    sprintf(input_filename, "in%d.txt", rank);

    FILE* input_file = fopen(input_filename, "r");
    ERR(!input_file, "Couldn't open peer input file");

    // Read owned files
    rc = fscanf(input_file, "%d", &data->peer_filecount);
    ERR(rc != 1, "Failed to read peer file count");

    for (int curr_file = 0; curr_file < data->peer_filecount; curr_file++) {
        rc = fscanf(input_file, "%s %d", 
            data->peer_filenames[curr_file], &data->peer_chunk_count[curr_file]);
        ERR(rc != 2, "Failed to read peer owned filename/line count");

        for (int curr_line = 0; curr_line < data->peer_chunk_count[curr_file]; curr_line++) {
            rc = fscanf(input_file, "%s", 
                data->peer_files[curr_file][curr_line]);
            ERR(rc != 1, "Failed to read peer owned file contents");
        }
    }

    // Read desired file names
    rc = fscanf(input_file, "%d", &data->desired_filecount);
    ERR(rc != 1, "Failed to read desired file count");

    for (int curr_file = 0; curr_file < data->desired_filecount; curr_file++) {
        rc = fscanf(input_file, "%s", data->desired_filenames[curr_file]);
        ERR(rc != 1, "Failed to read desired file names");
    }
}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    // Read and send initialization data
    read_peer_input(rank, &local_peer_data);
    MPI_Ssend(&local_peer_data, 1, MPI_peer_struct, TRACKER_RANK, INIT_TAG, MPI_COMM_WORLD);

    // Wait for start signal
    MPI_Recv(nullptr, 0, MPI_CHAR, TRACKER_RANK, START_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    Thread_arg args;
    args.rank = rank;
    args.numtasks = numtasks;

    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &args);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &args);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
}

void tracker(int numtasks, int rank)
{
    // Mapping filename -> set of peers/seeds
    std::unordered_map<std::string, std::set<int>> file_swarms;
    // Mapping filename -> pair<segment count, vector<hashes>>
    std::unordered_map<std::string, std::pair<int, std::vector<std::string>>> file_contents;
    Peer_data *peer_data = (Peer_data*) malloc(numtasks * sizeof(Peer_data));
    MPI_Status status;
    int peer_finish_counter = 0;

    // Receive initialization data from all peers
    for (int i = 0; i < numtasks; i++) {
        if (i == TRACKER_RANK)
            continue;

        MPI_Recv(&peer_data[i], 1, MPI_peer_struct, i, INIT_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    // Initialize file swarms
    for (int curr_peer = 0; curr_peer < numtasks; curr_peer++) {
        if (curr_peer == TRACKER_RANK)
            continue;

        for (int curr_file = 0; curr_file < peer_data[curr_peer].peer_filecount; curr_file++) {
            std::string filename = peer_data[curr_peer].peer_filenames[curr_file];

            file_swarms[filename].insert(curr_peer);
            file_contents[filename] = {peer_data[curr_peer].peer_chunk_count[curr_file], std::vector<std::string>()};

            for (int curr_hash = 0; curr_hash < peer_data[curr_peer].peer_chunk_count[curr_file]; curr_hash++) {
                file_contents[filename].second.push_back(peer_data[curr_peer].peer_files[curr_file][curr_hash]);
            }
        }
    }

    // Send start msg to all peers
    for (int i = 0; i < numtasks; i++) {
        if (i == TRACKER_RANK)
            continue;

        MPI_Ssend(nullptr, 0, MPI_CHAR, i, START_TAG, MPI_COMM_WORLD);
    }

    int source, tag, msg_len;
    int *swarm;
    void *msg_buffer;

    // Tracker logic loop, will stop upon receiving end message from all peersm
    while (peer_finish_counter < numtasks - 1)
    {
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        source = status.MPI_SOURCE;
        tag = status.MPI_TAG;

        switch(tag) {
            // Received request for the swarm of a file
            case SWARM_REQUEST_TAG:
                // Receive filename and its length
                MPI_Recv(&msg_len, 1, MPI_INT, source, SWARM_REQUEST_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                msg_buffer = malloc(msg_len);
                MPI_Recv(msg_buffer, msg_len, MPI_CHAR, source, SWARM_REQUEST_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // Add peer to swarm
                file_swarms[(char*)msg_buffer].insert(source);

                msg_len = file_swarms[(char*)msg_buffer].size();

                swarm = (int*) malloc(msg_len * sizeof(int));
                std::copy(file_swarms[(char*)msg_buffer].begin(), file_swarms[(char*)msg_buffer].end(), swarm);

                // Send file swarm, file length and peer count
                MPI_Ssend(&msg_len, 1, MPI_INT, source, SWARM_REQUEST_TAG, MPI_COMM_WORLD);
                MPI_Ssend(&file_contents[(char*)msg_buffer].first, 1, MPI_INT, source, SWARM_REQUEST_TAG, MPI_COMM_WORLD);
                MPI_Ssend(swarm, msg_len, MPI_INT, source, SWARM_REQUEST_TAG, MPI_COMM_WORLD);

                free(swarm);
                free(msg_buffer);
                break;

            // A peer has finished downloading a file
            case TRACKER_FILE_TAG:
                // Receive filename and filename length
                MPI_Recv(&msg_len, 1, MPI_INT, source, TRACKER_FILE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                msg_buffer = malloc(msg_len);
                MPI_Recv(msg_buffer, msg_len, MPI_CHAR, source, TRACKER_FILE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // Send the hashes of the segments to the peer
                for (int i = 0; i < file_contents[(char*)msg_buffer].first; i++) {
                    MPI_Ssend(file_contents[(char*)msg_buffer].second[i].data(), HASH_SIZE + 1, MPI_CHAR, source, TRACKER_FILE_TAG, MPI_COMM_WORLD);
                }

                break;

            // Received end signal from a peer download thread
            case END_TAG:
                MPI_Recv(nullptr, 0, MPI_CHAR, source, END_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                peer_finish_counter++;
                break;

            default:
                break;
        }
    }

    // Close all peer upload threads
    for (int i = 0; i < numtasks; i++) {
        if (i == TRACKER_RANK)
            continue;

        MPI_Send(nullptr, 0, MPI_CHAR, i, END_TAG, MPI_COMM_WORLD);
    }

    free(peer_data);
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Create MPI struct for the peer data used in the initialization step
    MPI_Datatype oldtypes[6];
    int blockcounts[6];
    MPI_Aint offsets[6];

    offsets[0] = offsetof(Peer_data, peer_filecount);
    oldtypes[0] = MPI_INT;
    blockcounts[0] = 1;

    offsets[1] = offsetof(Peer_data, peer_filenames);
    oldtypes[1] = MPI_CHAR;
    blockcounts[1] = MAX_FILES * (MAX_FILENAME + 1);

    offsets[2] = offsetof(Peer_data, peer_chunk_count);
    oldtypes[2] = MPI_INT;
    blockcounts[2] = MAX_FILES;

    offsets[3] = offsetof(Peer_data, peer_files);
    oldtypes[3] = MPI_CHAR;
    blockcounts[3] = MAX_FILES * MAX_CHUNKS * (HASH_SIZE + 1);

    offsets[4] = offsetof(Peer_data, desired_filecount);
    oldtypes[4] = MPI_INT;
    blockcounts[4] = 1;

    offsets[5] = offsetof(Peer_data, desired_filenames);
    oldtypes[5] = MPI_CHAR;
    blockcounts[5] = MAX_FILES * (MAX_FILENAME + 1);

    MPI_Type_create_struct(6, blockcounts, offsets, oldtypes, &MPI_peer_struct);
    MPI_Type_commit(&MPI_peer_struct);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Type_free(&MPI_peer_struct);

    MPI_Finalize();
}
