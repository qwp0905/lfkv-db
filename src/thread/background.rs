use std::{
  thread::{Builder, JoinHandle},
  time::Duration,
};

use crossbeam::channel::Sender;

use crate::{logger, ContextReceiver, StoppableChannel, UnwrappedSender};
// fn testest() {
//   StoppableThread::new(String::from("sdf"), 190, |v: usize| v);
// }

pub enum BackgroundJob<T, R> {
  New(fn(T) -> R),
  NewOrTimeout(Duration, fn(Option<T>) -> R),
  Done(fn(T) -> R),
  DoneOrTimeout(Duration, fn(Option<(T, Sender<R>)>)),
  All(fn(T) -> R),
}
impl<T, R> BackgroundJob<T, R>
where
  T: Send + 'static,
  R: Send + 'static,
{
  fn to_thread(
    &self,
    name: String,
    stack_size: usize,
    rx: ContextReceiver<T, R>,
  ) -> JoinHandle<()> {
    let builder = Builder::new().name(name).stack_size(stack_size);
    let t = match self {
      BackgroundJob::New(job) => {
        let job = job.clone();
        builder.spawn(move || {
          while let Ok(v) = rx.recv_new() {
            job(v);
          }
        })
      }
      BackgroundJob::NewOrTimeout(timeout, job) => {
        let job = job.clone();
        let timeout = *timeout;
        builder.spawn(move || {
          while let Ok(v) = rx.recv_new_or_timeout(timeout) {
            job(v);
          }
        })
      }
      BackgroundJob::Done(job) => {
        let job = job.clone();
        builder.spawn(move || {
          while let Ok((v, done)) = rx.recv_done() {
            let r = job(v);
            done.must_send(r)
          }
        })
      }
      BackgroundJob::DoneOrTimeout(timeout, job) => {
        let job = job.clone();
        let timeout = *timeout;
        builder.spawn(move || {
          while let Ok(v) = rx.recv_done_or_timeout(timeout) {
            job(v);
          }
        })
      }
      BackgroundJob::All(job) => {
        let job = job.clone();
        builder.spawn(move || {
          while let Ok((v, done)) = rx.recv_all() {
            let r = job(v);
            done.map(|tx| tx.must_send(r));
          }
        })
      }
    };

    t.unwrap()
  }
}

// pub struct BackgroundThread<T, R, F>
// where
//   F: Fn(T) -> R + Clone + 'static,
// {
//   _type: BackgroundJob,
//   channel: StoppableChannel<T, R>,
//   thread: Option<JoinHandle<()>>,
//   job: F,
//   name: String,
//   stack_size: usize,
// }

// impl<T, R, F> BackgroundThread<T, R, F>
// where
//   T: Send + 'static,
//   R: Send + 'static,
//   F: Fn(T) -> R + Send + Clone + 'static,
// {
// fn new(name: String, stack_size: usize, _type: BackgroundType<T, R>, job: F) -> Self {
//   let (channel, rx) = StoppableChannel::new();
//   let builder = Builder::new().name(name.clone()).stack_size(stack_size);
//   let cloned = job.clone();
//   Self {
//     _type,
//     channel,
//     thread: Some(thread),
//     job,
//     name,
//     stack_size,
//   }
// }
// }

// struct StoppableThread<T, R> {
//   channel: StoppableChannel<T, R>,
//   thread: JoinHandle<()>,
// }
// impl<T, R> StoppableThread<T, R>
// where
//   T: Send + 'static,
//   R: Send + 'static,
// {
//   fn new<F>(name: String, stack_size: usize, f: Box<F>) -> Self
//   where
//     F: Fn(T) -> R + Send + 'static + Clone,
//   {
//     let (channel, rx) = StoppableChannel::new();
//     let thread = Builder::new()
//       .name(name)
//       .stack_size(stack_size)
//       .spawn(move || {
//         while let Ok(v) = rx.recv_new() {
//           f(v);
//         }
//       })
//       .unwrap();
//     Self { channel, thread }
//   }

//   fn done<F>(name: String, stack_size: usize, f: Box<F>) -> Self
//   where
//     F: Fn(T) -> R + Send + 'static + Clone,
//   {
//     let (channel, rx) = StoppableChannel::new();
//     let thread = Builder::new()
//       .name(name)
//       .stack_size(stack_size)
//       .spawn(move || {
//         while let Ok((v, done)) = rx.recv_done() {
//           let r = f(v);
//           done.must_send(r);
//         }
//       })
//       .unwrap();
//     Self { channel, thread }
//   }
// }
// // impl<T, R> StoppableThread<T, R> {
// //   fn terminate(self) -> std::thread::Result<()> {
// //     if !self.thread.is_finished() {
// //       self.channel.terminate();
// //     }
// //     self.thread.join()
// //   }
// // }

// pub struct Background<T, R, F> {
//   thread: Option<StoppableThread<T, R>>,
//   name: String,
//   stack_size: usize,
//   job: Box<F>,
// }
// impl<T, R, F> Background<T, R, F>
// where
//   T: Send + 'static,
//   R: Send + 'static,
//   F: Fn(T) -> R + Send + 'static + Clone,
// {
//   pub fn new(name: String, stack_size: usize, f: F) -> Self {
//     let job = Box::new(f);
//     let thread = StoppableThread::new(name.clone(), stack_size, job.clone());
//     Self {
//       thread: Some(thread),
//       name,
//       stack_size,
//       job,
//     }
//   }

//   pub fn send(&mut self, t: T) {
//     if let Some(thread) = &mut self.thread {
//       if thread.thread.is_finished() {
//         *thread =
//           StoppableThread::new(self.name.clone(), self.stack_size, self.job.clone());
//       }
//       thread.channel.send(t);
//     }
//   }
// }
