/**
 *    MDBWrapper Apache2 Module Main Class
 *    Last Updated: 2/7/2012, By: Greg Franczyk
 *
 *    This module provides enhanced, 'wrapper' style, functionality to get & post requests made to a MongoDB
 *    instance through Apache.  The MDBWrapper directive is applied to one, or many, URI pattern(s) via the apache2
 *    configuration - this links each URI to a particular MongoDB database & collection.
 *
 *    The configuration directive (MDBWrapper) takes two parameters:
 *        (1) URI
 *        (2) File with configuration parameters (DB_COLLECTION,UNIQUE_FIELDS,UNREADABLE_FIELDS,DB_USERNAME,DB_PASSWORD)
 *
 *    The MDBWrapper request handler provides these special features:
 *        (1) Unique value constraints:
 *              To enforce unique values within a posted JSON, set the parameter UNIQUE_FIELDS in the config file
 *              with the unique keys to be enforced - keys may be composites, with field names separated by commas,
 *              multiple composite keys are supported and should be separated by pipes, ex:
 *                      UNIQUE_FIELDS = field1|field2,field3|_id
 *              The wrapper will verify that the values in the posted json, for the indicated fields, do not already
 *              exist in the destination db \ collection before inserting the posted object.
 *        (2) Augment post success response with the collection total count.
 *        (3) Unreadable fields:
 *              Set particular fields to be unreadable in get requests - this will effectively block access to the data
 *              stored in designated fields, ex:
 *                      UNREADABLE_FIELDS=_id
 *              The wrapper will prevent the key \ value pairs for the fields indicated from being included in the response.
 *        (4) Multiple Filter Support:
 *              The wrapper takes as an argument a set of values to query on, by including the parameter 'q' with a series
 *              of filters, ex:
 *                      ?q={"a":"1"}|*
 *              The result of all get requests to the mdbwrapper result in a json with a single q_results field - the
 *              value for this field is always an array with json's that match, sequentially, the q filter string - in the
 *              example above, the length of the q_results array would be 2 with the first json matching the more specific
 *              query and the second json matching the wildcard (* effectively signifies all results \ no filter - it's the
 *              default if no filter string is given).
 *
 */

// Includes..
#include "stdio.h"
#include "httpd.h"
#include "apr_pools.h"
#include "http_config.h"
#include "util_filter.h"
#include "http_protocol.h"
#include "mongo.h"
#include "json.h"
#include "http_log.h"

// Module declares itself.
module AP_MODULE_DECLARE_DATA mdbwrapper_module;

// Struct to handle configuration values.
typedef struct {
  char *uri;
  char *config_file;
  char *db_collection;
  size_t db_col_size;
  char *unique_fields;
  size_t unf_size;
  char *unreadable_fields;
  size_t unr_size;
  char *db_username;
  size_t db_un_size;
  char *db_password;
  size_t db_pw_size;
  int update_on_unique;
} mdbwrapper_config;

// Declaration of function to handle json to bson object transformations (implemented below).
static bson *json_to_bson (struct json_object *json, request_rec *r);


int replace_str(char *str, char *orig, char *rep)
{
  static char buffer[4096];
  char *p;
  if(!(p = strstr(str, orig)))
    return 0;
  strncpy(buffer, str, p-str);
  buffer[p-str] = '\0';
  sprintf(buffer+(p-str), "%s%s", rep, p+strlen(orig));
  memcpy(str,buffer,strlen(buffer)+1);
  return 1;
}

static void append_oid(bson *b, void *val, const char *key) {
  json_object * inner_val = json_object_object_get(val, "$oid");
  if(!inner_val)
    return;
  const char * oid_str = json_object_get_string(inner_val);
  if (!oid_str)
    return;
  bson_oid_t id_obj[1];
  bson_oid_from_string(id_obj, oid_str);
  bson_append_oid(b, key, id_obj);
}

/**
 * Appends the correct bson type to the passed in bson object: used during json parsing, called for each
 * individual key \ value pair.
 * @param b: The existing bson to append the new object to.
 * @param val: The value of the key\value pair parsed (void).
 * @param key: The character key for the key\value pair passed.
 * @param r: The apache request (used for apr memory allocation).
 * @return void: Result is appended to passed in bson.
 */
static void json_key_to_bson_key (bson *b, void *val,const char *key,request_rec *r) {
  if (strspn("$",key))
    return;
  // Determine the type of the passed in value and call the correct bson_append function.
  switch (json_object_get_type (val)) {
    case json_type_boolean:
      bson_append_bool (b, key, json_object_get_boolean (val));
      break;
    case json_type_double:
      bson_append_double (b, key, json_object_get_double (val));
      break;
    case json_type_int: {
      if (json_object_get_int64(val) > INT32_MAX)
        bson_append_long (b, key, json_object_get_int64 (val));
      else
        bson_append_int (b, key, json_object_get_int64 (val));
      break;
    }
    case json_type_object: {
      // The json value type has recursive behavior - calling back to the json_to_bson function to parse
      // the values internal to the nested json.
      if (strcmp("_id", key) == 0) {
        append_oid(b, val, key);
        break;
      }
      bson *sub;
      sub = json_to_bson (val, r);
      bson_append_bson(b,key,sub);
      bson_destroy (sub);
      break;
    }
    case json_type_array: {
      // To handle nested arrays.
      int pos;
      bson_append_start_array (b, key);
      for (pos = 0; pos < json_object_array_length (val); pos++) {
        char kk[10];
        sprintf(kk,"%d",pos);
        json_key_to_bson_key ( b, json_object_array_get_idx ( val, pos ), kk, r );
      }
      bson_append_finish_array (b);
      break;
    }
    case json_type_string: {
      const char * const strFromJSON = json_object_get_string(val);
      const char * strVal = strFromJSON;
      if (*strVal == '/' && strVal++) while(*strVal != '\0' && *strVal != '/') strVal++;
      if (*strVal == '/') {
        char * regex = (char*) apr_palloc(r->pool, strVal - strFromJSON - 1);
        for (int i = 1; i < strVal - strFromJSON; i++) *(regex+i-1) = *(strFromJSON+i);
        *(regex + (strVal - strFromJSON) - 1) = '\0';
        bson_append_regex(b, key, regex, ++strVal);
      }
      else bson_append_string (b, key, strFromJSON);
      break;
    }
    default:
      break;
  }
}

/**
 * Converts a json 'object' into a bson for insertion into the MongoDB.
 * @param json: The json object to be inserted.
 * @param r: The apache request (used for apr memory allocation).
 * @return bson: The converted bson object.
 */
static bson * json_to_bson (struct json_object *json, request_rec *r) {
  bson *loadBson = apr_palloc(r->pool,sizeof(bson));
  bson_init (loadBson);
  struct json_object_iter it;
  //Iterate over each value in the json, use json_key_to_bson_key to append the converted key\value pairs
  //to the bson we're building (loadBSON).
  json_object_object_foreachC(json, it)
    json_key_to_bson_key( loadBson, it.val, it.key, r );
  bson_finish (loadBson);
  return loadBson;
}

/**
 * Outputs the bson string response to the client.
 * @param r: The apache request
 * @param b_data: The pointer to the data for the bson to be converted
 * @param array: To indicate whether an array is being converted (determines bracket use).
 * @return void
 */
static void output_bson_string_response(request_rec *r, const char *b_data , int array ) {
  // Create a new bson iterator from the data pointer.
  bson_iterator i;
  bson_iterator_from_buffer( &i, b_data );
  // Decide whether we need a bracket or brace based on whether it's an array.
  if (!array)
    ap_rputs("{",r);
  else if (array)
    ap_rputs("[",r);
  // Use lt to track if it's the first value - don't want to add a comma before the first key.
  int lt = 0;
  // Iterate over all the bson components until we've printed out the whole thing.
  while ( bson_iterator_next( &i ) ) {
    // Get the type for the switch below.
    bson_type t = bson_iterator_type( &i );
    // If the type is 0 - somethings wrong, get out of here.
    if ( t == 0 ) break;
    // Print a comma to separate subsequent values.
    if (lt) ap_rputs(",",r);
    else lt++;
    // If it's not an array, write the name of the key.
    if (!array)
      ap_rprintf(r," \"%s\" : ",bson_iterator_key( &i ));
    // Declare pointer for String & Regex types - no initialized for other types.
    char *outStr = NULL;
    // Based on type - figure out how to output the value to the output buffer.
    switch ( t ) {
    case BSON_DOUBLE:
      ap_rprintf(r, "%f" , bson_iterator_double( &i ) );
      break;
    case BSON_REGEX:
      outStr = apr_palloc(r->pool,strlen(bson_iterator_regex(&i)) + 1);
      memcpy(outStr,bson_iterator_regex( &i ),strlen(bson_iterator_regex(&i)) + 1);
    case BSON_STRING: {
      if (!outStr) {
        outStr = apr_palloc(r->pool,strlen(bson_iterator_string(&i)) + 1);
        memcpy(outStr,bson_iterator_string( &i ),strlen(bson_iterator_string(&i)) + 1);
      }
      ap_escape_html(r->pool, outStr);
      ap_rprintf(r, "\"%s\"" , outStr );
      break;
    }
    case BSON_BOOL:
      ap_rprintf(r, "%s" , bson_iterator_bool( &i ) ? "true" : "false" );
      break;
    case BSON_DATE:
      ap_rprintf(r,"%ld" , ( long int )bson_iterator_date( &i ) );
      break;
    case BSON_INT:
      ap_rprintf(r, "%d" , bson_iterator_int( &i ) );
      break;
    case BSON_LONG:
      ap_rprintf(r, "%ld" , ( long int )bson_iterator_long( &i ));
      break;
    case BSON_OBJECT:
      output_bson_string_response(r, bson_iterator_value( &i ) , 0 );
      break;
    case BSON_ARRAY:
      output_bson_string_response(r, bson_iterator_value( &i ) ,1 );
      break;
    case BSON_OID: {
      char oidhex[25];
      bson_oid_to_string( bson_iterator_oid( &i ), oidhex );
      ap_rprintf(r, "{ \"$oid\" : \"%s\" }" , oidhex );
      break;
    }
    case BSON_TIMESTAMP: {
      bson_timestamp_t ts = bson_iterator_timestamp( &i );
      ap_rprintf(r,"i: %d, t: %d",ts.i, ts.t);
      break;
    }
    case BSON_NULL:
      ap_rputs("null",r);
      break;
    default:
      break;
    }
  }
  // Close out this iteration by using the proper brace or bracket.
  if(array) ap_rputs("]",r);
  else if(!array) ap_rputs("}",r);
}

/**
 * Performs the insertion into the MongoDB, finds the collection count, prepares the response,
 * closes the connection to the MongoDB (destroys the connection object).
 * @param postJSON: The json object to be inserted (parsed from post content).
 * @param conn: The mongoDB connection started for this request.
 * @param db_collection: The name of the db & collection (format: db.collection).
 * @param r: The Apache request data struct.
 * @return void.
 *
 */
static void insert_json_to_db(struct json_object *postJSON, mongo *conn,
                              char* db_collection, request_rec *r) {
  // Convert the json to a bson for use with the MongoDB C driver.
  bson *b = json_to_bson(postJSON, r);
  // Perform the insert operation.
  int status = mongo_insert( conn,db_collection , b, NULL );
  // Destroy the bson - we're done with it.
  bson_destroy(b);
  // Perpare error response if insert failed & return.
  if (status != MONGO_OK) {
    ap_rputs("{\"ok\":false,\"reason\":\"Error inserting into the database.\"}",r);
    //Destory the connection - we don't need it anymore - return.
    mongo_destroy(conn);
    return;
  }
  //Split the db & collection into two separate char arrays for mongo_count function.
  char *dbc[2];
  dbc[0] = strtok(db_collection,".");
  dbc[1] = strtok(NULL,".");
  //Fetch the collection count from the MongoDB.
  int count = mongo_count(conn, dbc[0],dbc[1],NULL);
  //Perpare the success response.
  ap_rputs("{\"ok\":true, \"count\":",r);
  ap_rprintf(r, "%i",count);
  ap_rputs(", \"time\":",r);
  ap_rprintf(r, "%ld", r->request_time);
  ap_rputs("}",r);
  //Destroy the connection & return NULL for void function (shuts up the compiler).
  mongo_destroy(conn);
}

/**
 * Performs the get operation - the logic to build the query \ filters is in this function,
 * printing of the db result rows to the client is delegated to output_bson_string_response.
 * @param r: The Apache request data struct.
 * @param postJSON: The json object to be inserted (parsed from post content).
 * @param conn: The mongoDB connection started for this request.
 * @param db_collection: The name of the db & collection (format: db.collection).
 * @param unreadable: The unreadable field names.
 * @param request_args: The request paramter values.
 * @param arg_params: The number of request arguments.
 * @return void.
 *
 */
static void perform_get_request_handler(request_rec *r, char *db_collection,
                                        const char *db_username, const char *db_password,
                                        char *unreadable, char **request_args,
                                        int arg_params) {
  // Connect to the mongo db.
  mongo conn[1];
  int status = mongo_client( conn, "127.0.0.1", 27017 );
  // If we cannot create the connection, log an error, send back an error, close the db connection.
  if( status != MONGO_OK ) {
    switch ( conn->err ) {
    case MONGO_CONN_SUCCESS:
      fprintf(stderr, "connection succeeded\n" );
      break;
    case MONGO_CONN_NO_SOCKET:
      fprintf(stderr, "no socket\n" );
      break;
    case MONGO_CONN_FAIL:
      fprintf(stderr, "connection failed\n" );
      break;
    case MONGO_CONN_NOT_MASTER:
      fprintf(stderr, "not master\n" );
      break;
    }
    mongo_destroy(conn);
    ap_rputs("{\"ok\":false,\"reason\":\"Could not accquire a connection to the db.\"}", r);
    return;
  }
  char* db_col_cpy = apr_palloc(r->pool,strlen(db_collection)+1);
  strcpy(db_col_cpy,db_collection);
  if (strlen(db_username) != 0 && strlen(db_password) != 0) {
    status = mongo_cmd_authenticate( conn, strtok(db_col_cpy,"."), db_username, db_password );
    if( status != MONGO_OK ) {
      mongo_destroy(conn);
      ap_rputs("{\"ok\":false,\"reason\":\"Authentication Error - Could not accquire a connection to the db.\"}", r);
      return;
    }
  }
  // Get the relevant 'get' parameters from the arguments map.
  char *queryParams = NULL;
  int limit = 0;
  int skip = 0;
  for (int k = 0; k <= arg_params; k++) {
    char *key = strtok(request_args[k],"=");
    char *val = strtok(NULL,"=");
    if(strcmp(key,"q") == 0)
      queryParams = val;
    else if (strcmp(key,"l") == 0)
      limit = atoi(val);
    else if (strcmp(key,"s") == 0)
      skip = atoi(val);
  }
  
  // If we didn't get any queryParams, create a default value of *.
  if (!queryParams) {
    queryParams = apr_palloc( r->pool, 2 );
    memcpy( queryParams,"*\0", 2 );
  }


  char *queries[20];
  int cp = 0;
  queries[0] = strtok(queryParams,"|");
  // Iterate over the queries to build and get the values in the query array **up to 20**.
  while (cp < 20) {
    char *parse = strtok(NULL,"|");
    if (!parse) break;
    cp++;
    queries[cp] = parse;
  }
  // Create the fields bson to filter out the unreadable fields.
  // This is used by the cursor below.
  bson fields[1];
  mongo_cursor cursor[1];
  bson_init(fields);
  char *filter_field = NULL;
  filter_field = strtok(unreadable,",");
  const int n = 0;
  while (filter_field) {
    bson_append_int(fields,filter_field,0);
    filter_field = strtok(NULL,",");
  }
  bson_finish(fields);
  bson fullResult[1];
  // JSONP support
  // Start to print the result set
  ap_rputs("{ \"q_results\" : [", r);
  for (int ib = 0; ib <= cp; ib++) {
    mongo_cursor_init( cursor, conn, db_collection );
    mongo_cursor_set_fields(cursor,fields);
    // Convert each individual query string into a json, then a bson, which is used by the cursor.
    bson *queryStr = apr_palloc(r->pool,sizeof(bson));
    // If the value is wildcard, we shouldn't try to serialize it into a bson.
    if(strcmp(queries[ib],"*") != 0) {
      ap_unescape_url(queries[ib]);
      struct json_object *queryJSON = json_tokener_parse (queries[ib]);
      if(queryJSON && json_object_is_type(queryJSON,json_type_object)) 
        queryStr = json_to_bson(queryJSON,r);
      else {
        // If the filter we got was not wildcard and not a valid JSON - just ignore it.
        fprintf(stderr,"There was an error decoding the query JSON :: Ignoring the query \n");
        bson_init(queryStr);
        bson_finish(queryStr);
      }
    } else {
      bson_init(queryStr);
      bson_finish(queryStr);
    }
    // Set the query bson - if it's empty, that's ok, it'll just fetch all results.
    mongo_cursor_set_query(cursor,queryStr);
    // Print the offset info & rows field name at the beginning of this query result json.
    if (ib > 0)
      ap_rputs(", ",r);
    ap_rprintf(r,"{\"offset\" : %d, \"rows\" : [",skip);
    // User counters to make sure we skip and include the right amount of rows.
    int counter = 0;
    int skp_cnter = 0;
    while(  mongo_cursor_next( cursor ) == MONGO_OK) {
      if (skp_cnter >= skip) {
        if (counter > 0)
          ap_rputs(", ",r);
        output_bson_string_response(r,bson_data(mongo_cursor_bson(cursor)),0);
        counter++;
        if(limit > 0 && counter >= limit)
          break;
      }
      skp_cnter++;
    }
    // Finish up this particular json query result object.
    mongo_cursor_destroy(cursor);
    ap_rprintf(r,"], \"total_rows\" : %d, \"query\" : ",counter);
    output_bson_string_response(r,bson_data(queryStr),0);
    ap_rputs("}",r);
    bson_destroy(queryStr);
  }
  // Finish the full response.
  ap_rprintf(r,"], \"time\" : %ld}", r->request_time);
  bson_destroy(fields);
  mongo_destroy(conn);
}
/**
 * Callback function registered as the handler for all HTTP requests.
 * @param r: Apache request data struct.
 * @param data: Passed in data (not used - implemented as a part of callback function).
 * @return int: ENUM that directs Apache on how to proceed after this logic completes.
 */
static int mod_mdbwrapper_method_handler (request_rec *r) {
  // For HTTP verbs besides GET \ POST, invalid - bounce back.
  if (r->method_number != M_POST && r->method_number != M_GET)
    return OK;
  // Get the configuration values for the module (see configuration callback below).
  mdbwrapper_config *s_cfg = ap_get_module_config(r->server->module_config, &mdbwrapper_module);
  // Check to see if this is a URI that should be MDMWrapper wrapped.
  int i = 0;
  int cont = 0;
  while(s_cfg[i].uri != NULL && i < 100) {
    if (strcmp(s_cfg[i].uri,r->uri) == 0) {
      cont = 1;
      break;
    }
    i++;
  }
  // If not, get out of the way.
  if (!cont)
    return DECLINED;
  // Get all of the query parameters for the request & add them to the args array - just to have args ready for future use.
  // Use char array length of 100 to be safe - parameters 101 & above will be chopped.
  char *args[100];
  int arg_params = -1;
  // Use strtok to tokenize the args over ampersand.
  if (r->args)
    args[0] = strtok(r->args,"&");
  else
    args[0] = NULL;
  // Set up our uniqueKey char buffer in advance.
  if(args[0] != NULL) {
    arg_params++;
    // Strtok until we tokenize all the args (when parse is null).
    while (arg_params < 100) {
      char *parse = strtok(NULL,"&");
      if (!parse) break;
      arg_params++;
      args[arg_params] = parse;
    }
  } else arg_params=-1;
  char *db_collection = apr_palloc(r->pool,s_cfg[i].db_col_size);
  memcpy(db_collection,s_cfg[i].db_collection,s_cfg[i].db_col_size);
  char *db_username = apr_palloc(r->pool,s_cfg[i].db_un_size);
  memcpy(db_username,s_cfg[i].db_username,s_cfg[i].db_un_size);
  char *db_password = apr_palloc(r->pool,s_cfg[i].db_pw_size);
  memcpy(db_password,s_cfg[i].db_password,s_cfg[i].db_pw_size);
  char *jsonp = NULL;
  int update_on_unique = s_cfg[i].update_on_unique;
  for (int k = 0; k <= arg_params; k++) {
    char *strToParse = apr_palloc(r->pool,512);
    memcpy(strToParse,args[k],strlen(args[k])+1);
    char *key = strtok(strToParse,"=");
    char *val = strtok(NULL,"=");
    if (strcmp(key,"jsonp") == 0) {
      jsonp = val;
      break;
    }
  }
  if(jsonp) {
    ap_rputs(jsonp,r);
    ap_rputs("(",r);
  }
  if(r->method_number == M_GET) {
    char *unreadable = apr_palloc(r->pool,s_cfg[i].unr_size);
    memcpy(unreadable,s_cfg[i].unreadable_fields,s_cfg[i].unr_size);
    perform_get_request_handler(r,db_collection,db_username,db_password,unreadable,args,arg_params);
    if(jsonp)
      ap_rputs(")",r);
    return OK;
  }
  char *unique;
  if (s_cfg[i].unique_fields) {
    unique = apr_palloc(r->pool,s_cfg[i].unf_size);
    memcpy(unique,s_cfg[i].unique_fields,s_cfg[i].unf_size);
  }
  // Create a new bucket brigade for pulling in the post data.
  apr_bucket_brigade* brigade = apr_brigade_create(r->pool, r->connection->bucket_alloc);
  // Max postData size is 16384 for now.
  // TODO: Change this to equal content-length in HTTP Request Header - if content-lenght is null, bounce back
  // the request as invalid.
  char *postData = apr_palloc(r->pool,16384);
  // Set the character limit of postData for use with the logic below.
  int count_bytes = 16384;
  // Variable to track progress of filling the postData buffer.
  int tlen = 0;
  // Process the request in 8192 char chunks for segmentation of large posts.
  apr_size_t len = 8192;
  // Get a pointer for ap_get_brigade (below).
  apr_size_t *ptlen = &len;
  // Read the request input brigade in 8192 byte chunks - do this until we've read the whole thing.
  while (ap_get_brigade(r->input_filters, brigade,
                        AP_MODE_READBYTES, APR_BLOCK_READ, len) == APR_SUCCESS) {
    // Allocate a new buffer for this iteration.
    char *buf = apr_palloc(r->pool,8192);
    // Extract the character content from the brigade - upto 8192 (the length we set above).
    apr_brigade_flatten(brigade, buf, ptlen);
    // We're done reading - destroy the brigade & content.
    apr_brigade_destroy(brigade);
    // If we've exceeded the post data maximum (defined above) or len is 0 (we've read it all), then we're done, break.
    if ((tlen + len) > count_bytes || !len)
      break;
    // Load the postData buffer by copying from the buf buffer that was created for this iteration.
    memcpy(postData+tlen,buf,len);
    // Incrememnt our total length by the amount read in this iteration.
    tlen += len;
    // Reset len value for the next iteration.
    // TODO: Tweak this logic - currently we could theorically read over our defined limit of 8192, although
    // it's impossible practically. Will be addressed with content-length change from above.
    len = count_bytes - tlen;
  }
  // Just in case there was no post content.
  if (tlen == 0) {
    ap_rputs("{\"ok\":false,\"reason\":\"No data was posted.\"}",r);
    if(jsonp)
      ap_rputs(")",r);
    return OK;
  }
  char *uniqueCompositeKey[20];
  int uck_counter = -1;
  uniqueCompositeKey[0] = strtok(unique,"|");
  if (uniqueCompositeKey[0] != NULL) {
    uck_counter += 2;
    while (uck_counter < 20) {
      char *parse = strtok(NULL,"|");
      if (!parse) break;
      uniqueCompositeKey[uck_counter] = parse;
      uck_counter++;
    }
  }
  // Convert our post data into a json object.
  struct json_object *postJSON = json_tokener_parse (postData);
  if(!postJSON || !json_object_is_type(postJSON,json_type_object)) {
    fprintf(stderr,"There was an error decoding the JSON :: Exiting \n");
    ap_rputs("{\"ok\":false,\"reason\":\"JSON could not be properly parsed.\"}",r);
    if(jsonp)
      ap_rputs(")",r);
    return OK;
  }
  // Initialize the mongoDB connection.
  mongo conn[1];
  int status = mongo_client( conn, "127.0.0.1", 27017 );
  // If we cannot create the connection, log an error, send back an error, close the db connection.
  if( status != MONGO_OK ) {
    switch ( conn->err ) {
    case MONGO_CONN_SUCCESS:
      fprintf(stderr, "connection succeeded\n" );
      break;
    case MONGO_CONN_NO_SOCKET:
      fprintf(stderr, "no socket\n" );
      break;
    case MONGO_CONN_FAIL:
      fprintf(stderr, "connection failed\n" );
      break;
    case MONGO_CONN_NOT_MASTER:
      fprintf(stderr, "not master\n" );
      break;
    }
    mongo_destroy(conn);
    ap_rputs("{\"ok\":false,\"reason\":\"Could not accquire a connection to the db\"}",r);
    if(jsonp)
      ap_rputs(")",r);
    return OK;
  }
  char* db_col_cpy = apr_palloc(r->pool,strlen(db_collection)+1);
  strcpy(db_col_cpy,db_collection);
  if (s_cfg[i].db_un_size != 0 && s_cfg[i].db_pw_size != 0) {
    status = mongo_cmd_authenticate( conn, strtok(db_col_cpy,"."), db_username, db_password );
    if( status != MONGO_OK ) {
      ap_rputs("{\"ok\":false,\"reason\":\"Authentication Error - Could not accquire a connection to the db.\"}", r);
      mongo_destroy(conn);
      json_object_put(postJSON);
      if(jsonp)
        ap_rputs(")",r);
      return OK;
    }
  }
  
  // If we didn't have a unique key, no worries, just insert the document & return with success.
  if (uck_counter < 0) {
    insert_json_to_db(postJSON,conn,db_collection,r);
    json_object_put(postJSON);
    if(jsonp)
      ap_rputs(")",r);
    return OK;
  }

  int result = 0;
  for(int ick = 0; ick < uck_counter; ick++) {
    char *uniqueKey[20];
    int uk_counter = -1;
    uniqueKey[0] = strtok(uniqueCompositeKey[ick],",");
    if (uniqueKey[0] != NULL) {
      uk_counter += 2;
      while (uk_counter < 20) {
        char *parse = strtok(NULL,",");
        if (!parse) break;
        uniqueKey[uk_counter] = parse;
        uk_counter++;
      }
    }
    // Extract the value of the unique key from the posted json.
    bson query[1];
    bson_init( query );
    int query_valid = 0;
    for(int ik = 0; ik < uk_counter; ik++) {
      json_object *jsonUniqueKey = json_object_object_get(postJSON,uniqueKey[ik]);
      if (jsonUniqueKey) {
        json_key_to_bson_key( query, jsonUniqueKey, uniqueKey[ik], r );
        query_valid = 1;
      }
    }
    // If the unique key was not present in the posted json, no worries, just insert the doc & return with success.
    if (!query_valid) continue;
    // Prepare the query & check the mongoDB collection for the key\value pair.
    bson_finish( query );
    mongo_cursor cursor[1];

    mongo_cursor_init( cursor, conn, db_collection );
    mongo_cursor_set_query( cursor, query );
    // Count the results - if there is even one, break & fail on unique constraint.
    while( mongo_cursor_next( cursor ) == MONGO_OK ) {
      if (!update_on_unique) {
    	  result++;
    	  break;
      }
      else 
    	  mongo_remove(conn, db_collection,&cursor->current, NULL);
    }
    // Done querying
    bson_destroy(query);
    // Done with the cursor
    mongo_cursor_destroy(cursor);
  }
  // If there are no results - great, insert the doc & return success.
  if (!result) {
    insert_json_to_db(postJSON,conn,db_collection,r);
    json_object_put(postJSON);
    if(jsonp)
      ap_rputs(")",r);
    return OK;
  }
  // If there are results - send back an error, destroy the db connection.
  mongo_destroy( conn );
  ap_rputs("{\"ok\":false,\"reason\":\"Unique field already exists.\"}",r);
  json_object_put(postJSON);
  if(jsonp)
    ap_rputs(")",r);
  return OK;
}

/**
 * This callback function is registered with module data below - it registers our handler above.
 * @param p: The Apache request pool.
 */
static void mod_mdbwrapper_register_hooks (apr_pool_t *p) {
  // Hook our module handler function - make it first in the handler sequence.
  // TODO: Don't really this APR_HOOK_REALLY_FIRST is required, APR_HOOK_FIRST is *probably* enough.
  ap_hook_handler(mod_mdbwrapper_method_handler, NULL, NULL, APR_HOOK_REALLY_FIRST);
}

/**
 * Callback function that's called when our MDBWrapper configuration directives are parsed.
 */
static const char *set_mdbwrapper_string(cmd_parms *parms, void *mconfig, const char *arg1, const char *arg2) {
  // Fetch module configuration.
  mdbwrapper_config *s_cfg = ap_get_module_config(parms->server->module_config, &mdbwrapper_module);
  static int i = 0;
  // Insert the configuration values into our module config. object.
  s_cfg[i].uri = (char *) arg1;
  s_cfg[i].config_file = ( char *) arg2;
  // Fetch the values from the config file and load them into the module data structure
  FILE *file = NULL;
  if (s_cfg[i].config_file)
    file = fopen ( s_cfg[i].config_file, "r" );
  else {
    // If we can't open the file - print an error message & ignore this particular directive.
    fprintf(stderr,"File name is not specified properly - ignoring the directive.");
    s_cfg[i].uri = NULL;
    return NULL;
  }
  if ( file != NULL ) {
    char line [256];
    while ( fgets ( line, sizeof line, file ) != NULL ) {
      char *key = strtok(line,"=");
      char *val = strtok(NULL,"=");
      if (strcmp(key,"DB_COLLECTION") == 0 && val) {
        s_cfg[i].db_col_size = (strlen(val));
        char *nlptr = strchr(val, '\n');
        if (nlptr) *nlptr = '\0';
        s_cfg[i].db_collection = malloc(s_cfg[i].db_col_size);
        memcpy(s_cfg[i].db_collection,val,s_cfg[i].db_col_size);
      }
      else if(strcmp(key,"UNIQUE_FIELDS") == 0 && val) {
        s_cfg[i].unf_size = (strlen(val));
        char *nlptr = strchr(val, '\n');
        if (nlptr) *nlptr = '\0';
        s_cfg[i].unique_fields = malloc(s_cfg[i].unf_size);
        memcpy(s_cfg[i].unique_fields,val,s_cfg[i].unf_size);
      }
      else if(strcmp(key,"UNREADABLE_FIELDS") == 0 && val) {
        s_cfg[i].unr_size = (strlen(val));
        char *nlptr = strchr(val, '\n');
        if (nlptr) *nlptr = '\0';
        s_cfg[i].unreadable_fields = malloc(s_cfg[i].unr_size);
        memcpy(s_cfg[i].unreadable_fields,val,s_cfg[i].unr_size);
      }
      else if(strcmp(key,"DB_USERNAME") == 0 && val) {
        s_cfg[i].db_un_size = (strlen(val));
        char *nlptr = strchr(val, '\n');
        if (nlptr) *nlptr = '\0';
        s_cfg[i].db_username = malloc(s_cfg[i].db_un_size);
        memcpy(s_cfg[i].db_username,val,s_cfg[i].db_un_size);
      }
      else if(strcmp(key,"DB_PASSWORD") == 0 && val) {
        s_cfg[i].db_pw_size = (strlen(val));
        char *nlptr = strchr(val, '\n');
        if (nlptr) *nlptr = '\0';
        s_cfg[i].db_password = malloc(s_cfg[i].db_pw_size);
        memcpy(s_cfg[i].db_password,val,s_cfg[i].db_pw_size);
      }
      else if(strcmp(key,"UPDATE_ON_DUPLICATE") == 0 && val) {
        if (strcmp(val,"true"))
        	s_cfg[i].update_on_unique = 1;
      }
    }
    fclose ( file );
    // Increment the static counter so we don't override the first value over & over.
  }
  if (!file || !s_cfg[i].db_collection || !s_cfg[i].db_username) {
    // If we can't open the file or the db_collection is not defined - print an error message & ignore this particular directive.
    // We won't fail loading the module for a single improperly configured directive, just move on.
    fprintf(stderr,"Could not parse the configuration file - ignoring the directive.");
    s_cfg[i].uri = NULL;
    return NULL;
  }
  i++;
  // Return success - no news is good news.
  return NULL;
}

/**
 * A declaration of the configuration directives that are supported by this module.
 */
static const command_rec mod_mdbwrapper_cmds[] = {
  AP_INIT_ITERATE2(
    "MDBWrapper",
    set_mdbwrapper_string,
    NULL,
    RSRC_CONF,
    "MDMWrapper <uri> <db_collection> -- uri to db \\ collection mapping"),
  {NULL}
};

/**
 * Initialize the module confguration struct & allocate memory for it.
 */
static void *create_mdbwrapper_config(apr_pool_t *p, server_rec *s)
{
  // This module's configuration structure.
  mdbwrapper_config *newcfg;
  // Allocate memory from the provided pool.
  newcfg = (mdbwrapper_config *) apr_pcalloc(p, sizeof(mdbwrapper_config)*100);
  // Return the created configuration struct.
  return (void *) newcfg;
}

/**
 * Declare and populate the module's data structure.
 */
module AP_MODULE_DECLARE_DATA mdbwrapper_module =
{
  STANDARD20_MODULE_STUFF, // Standard config.
  NULL, // Per-Directory Configuration Structures - don't need (yet?).
  NULL, // Merge Per-Directory - nothing to merge.
  create_mdbwrapper_config, // Create Per-Server Configuration Structures.
  NULL, // Merge Per-Server, I don't know when we would ever use this..
  mod_mdbwrapper_cmds, // MDBWrapper Directive Handler.
  mod_mdbwrapper_register_hooks, // Request Handler - to add our hook.
};
