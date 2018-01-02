import async from 'async';
import logger from './logger';

/*
 * @Usage
 * let q = new Queue(fs.readFile, 10);
 * 
 * q.forEach((file_name, content, done) => {
 *   done(errorIfAny); or done();
 * }).onFilled((files, done) => {
 *   console.log(files);  // ['a.txt', 'b.txt', 'c.txt']
 * }).start();
 *
 * q.push('a.txt').push('b.txt').push('c.txt')
 *
 * q.filled();
 *
 *
 * @Variables
 * op : Function that is to be executed for each Task
 * concurrency : Number of task that are allowed to process in parellel.
 * _q : Instance of async.queue
 * _size : Tasks that are currently in queue to process.
 * _isKilled : Flag variable in case any error occurred and queue is killed.
 * _packed : Flag variable when queue is completely filled.
 * _executed : Number of tasks that have been successfully processed.
 * _tasks : An array of all the elements that are allowed for queue processing
 *
 * @Hooks
 * pushIf : A check before adding task to queue
 * onFilled : Lists all the tasks in queue, when queue is filled.
 * forEach : Result of every task is passed to forEach hook
 * first : Only called when first result is generated.
 * completed : When queue is declared filled and everything is processed.
 * catch : When error occurs queue is immediately stopped and catch hook is passed the error
 *
 * FAQ:
 * Q - What are Hooks?
 * A - Hooks are function registered for queue
 *
 * Q - Why _size and _executed both defined?
 * A - Complete length of queue is not known, until it is declared as filled.
 *
 * Q - How many hooks can be added to a queue?
 * A - Only one function can be register for each hook
 */
export default class Queue {

  constructor(op, concurrency = 1) {
    this._q = async.queue(
      (ob, done) =>
        op(ob).catch(error => done(error)).then(data => done(null, data)).catch(error => this._processingError(error)),
      concurrency);
    this._q.pause();
    this._size = 0;
    this._isKilled = false;
    this._isStarted = false;
    this._packed = false;
    this._executed = 0;
    this._preStartTasks = [];
    this._tasks = [];
  }

  push(options = {}) {
    if(this._packed)
      throw new Error('Queue is completely filled.');
    !this._isStarted ? this._preStartTasks.push(options) : this._push(options);
    return this;
  }

  _push(options = {}) {
    if(this._isKilled)
      throw new Error('Queue is Killed');

    if(this._pushIf && !this._pushIf(options)) return;
    this._tasks.push(options);
    this._size++;
    this._q.push(options, (errorOnClientOperation, page) => {
      this._processingError(errorOnClientOperation)
      if(this._isKilled) return;
      if(this._onFirstPage && !this._executed++) {
        this._onFirstPage(page, this._processingError);
      }
      this._onPage(options, page, (errorOnProcessing) => {
        if(errorOnProcessing)
          return this._processingError(errorOnProcessing)
        this._size--;
        this._processed();
      });
    });
  }

  filled() {
    this._packed = true;
    if(this._onFilled) {
      this._onFilled(this._tasks, this._processingError);
    }
    if(this._isStarted) this._processed();
    return this;
  }

  first(_onFirstPage) {
    this._onFirstPage = _onFirstPage;
    return this;
  }

  pushIf(_pushIf) {
    this._pushIf = _pushIf;
    return this;
  }

  onFilled(_onFilled) {
    this._onFilled = _onFilled;
    return this;
  }

  forEach(_onPage) {
    this._onPage = _onPage;
    return this;
  }

  completed(_onComplete) {
    this._onComplete = _onComplete;
    return this;
  }

  catch(_onError) {
    this._onError = _onError;
    return this;
  }

  start() {
    this._preStartTasks.forEach(task => this._push(task));
    if(this._packed && this._onFilled) {
      this._onFilled(this._tasks, this._processingError);
    }
    this._q.resume();
    this._isStarted = true;
    this._processed();
    return this;
  }

  _processed() {
    if(this._packed && this._size == 0) {
      this._isKilled = true;
      this._q.kill();
      if(this._onComplete)
        this._onComplete();
    }
  }

  _processingError(errorOnQueueProcessing) {
    if(this._isKilled) return;
    if(!errorOnQueueProcessing) return;
    this._isKilled = true;
    this._q.kill();
    logger.error(errorOnQueueProcessing);
    if(this._onError)
      this._onError(errorOnQueueProcessing);
  }
}
