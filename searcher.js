// Created with love by Erick Aky
// This is a bridge between ElasticSearch and Firebase

const elasticsearch = require('elasticsearch');
const conf = require('./config');
const admin = require('firebase-admin');
require('colors');

const escOptions = {
  hosts: [{
    host: conf.ES_HOST,
    port: conf.ES_PORT,
    auth: (conf.ES_USER && conf.ES_PASS) ? conf.ES_USER + ':' + conf.ES_PASS : null
  }]
};


/************************************************
 INIT ELASTIC SEARCH
 ***********************************************/

const esc = new elasticsearch.Client(escOptions);

console.log('Connecting to ElasticSearch host %s:%s'.grey, conf.ES_HOST, conf.ES_PORT);

let timeoutObj = setTimeout(function() {
  esc.ping()
    .then(function() {
      console.log('Connected to ElasticSearch host %s:%s'.grey, conf.ES_HOST, conf.ES_PORT);
      clearInterval(timeoutObj);
    })
    .catch(console.info)
}, 100);


/************************************************
 INIT FIREBASE
 ***********************************************/

const config = {
  databaseURL: conf.FB_URL,
  credential: admin.credential.cert(conf.FB_SERVICEACCOUNT)
};
admin.initializeApp(config);

admin.database()
  .ref('/search/request')
  .on('child_added', (data) => {

    let request = data.val();

    if (!request.userId) {
      console.info('empty userId');
      return;
    }

    if (!request.in) {
      console.info('empty in');
      return;
    }

    if (!request.query) {
      console.info('empty query');
      return;
    }

    console.log('search for'.grey, request.query, request.in.grey);

    search(request)
      .then(response => {
        // remap
        response = response.map(item => item.hits);

        console.log('found: '.grey, response.map(item => item.hits.length).reduce((a, b) => a + b));

        // check visibility
        checkVisibility(request, response)
          .then(response => {
            console.log('returned: '.grey, response.map(item => item.hits.length).reduce((a, b) => a + b));

            admin.database().ref('/search/response/' + data.key).set(response);
          });

      })
      .catch(error => {
        console.info(error);
      })

  });

function checkVisibility(request, response) {
  let types = request.in.split(',');
  let userId = request.userId;

  return Promise.all(
    types.map((type, index) => filterByType(response[index].hits, type.trim(), userId))
  )
    .then(data => {
      return data.map((hits, index) => {
        return {
          hits: hits || [],
          max_score: response[index].max_score,
          total: response[index].total
        }
      });
    });
}

function filterByType(hits, type, userId) {
  switch (type) {
    case 'post': {
      return filterAsync(hits, hit => checkPostVisibility(hit._source, userId));
    }
    case 'user': {
      return filterAsync(hits, hit => {
        // put userId to the source
        hit._source.userId = hit._id;

        return checkUserVisibility(hit._source, userId)
      });
    }
    case 'interest': {
      return hits;
    }
  }
}

function checkPostVisibility(post, userId) {
  // hide hidden posts
  if (post.hidden) {
    return false;
  }

  if (userId === post.userId) {
    return true;
  }

  // hide offensive posts
  if (post.moderationType && post.moderationType === 'offensive') {
    return false;
  }

  if (post.visibility) {
    switch (post.visibility.type) {
      case 'inner-circle': {
        return getValue(`inner-circle/${post.userId}/${userId}`);
      }
      case 'public': {
        return true;
      }
      case 'only-me': {
        return false;
      }
      case 'only-selected-circles': {
        if (post.visibility.value && Array.isArray(post.visibility.value)) {
          return Promise.all(
            post.visibility.value.map(circleId => getValue(`circle-users/${post.userId}/${circleId}/${userId}`))
          )
            .then(results => ~results.indexOf(true))
        }
        break;
      }
      case 'only-selected-users': {
        if (post.visibility.value && Array.isArray(post.visibility.value)) {
          return ~post.visibility.value.indexOf(userId);
        }
      }
    }
  }
}

function checkUserVisibility(user, userId) {
  if (userId === user.userId) {
    return true;
  }

  // hide hidden users
  if (user.isBlocked) {
    return false;
  }

  const EVERYONE = 1;
  const INNER_CIRCLE = 2;

  // check "who can look me up option"
  return getValue(`privacy-look-me/${user.userId}`)
    .then(value => {
      if (!value || value == EVERYONE) {
        return true;
      }

      if (value === INNER_CIRCLE) {
        return getValue(`inner-circle/${user.userId}/${userId}`);
      }
    });
}

function getValue(path) {
  return new Promise((resolve, reject) => {
    admin.database().ref(path).once('value',
      snapshot => resolve(snapshot.val()),
      error => reject(error)
    );
  });
}

function search(request) {
  // prepare search request
  let types = request.in.split(',');
  let searches = [];
  let field = request.field || 'name';

  for (let type of types) {
    searches.push({'index': 'staging', 'type': type.trim()});
    let match = {};
    match[field] = request.query;
    searches.push({'query': {'match': match}, 'from': 0, 'size': 30});
  }

  return esc.msearch({
    body: searches
  })
    .then(data => data.responses)
}

function filterAsync(array, filter) {
  return Promise.all(array.map(entry => filter(entry)))
    .then(bits => array.filter(entry => bits.shift()));
}



