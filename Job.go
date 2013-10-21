package main

import (
	"io"
	"log"
	"os"
	"os/exec"
	"time"
	"sync/atomic"
)

type ShellJob struct {
	name         string
	execute      string
	directory    string
	environments []string
	arguments    []string
	logfile      string
	timeout      time.Duration
	expression   string
	status       int32
}

func (self *ShellJob) Run() {
	if !atomic.CompareAndSwap(&self.status, 0, 1) {
		log.Println("["+self.name+"] running!")
		return 
	}

	go func() {
       defer atomic.StoreInt32(&self.status, 0)
       e := self.rotate_file()
       if nil != e {
		log.Println("["+self.name+"] rotate log file failed,", e)
       }
       self.do_run()
	}()
}

func (self *ShellJob) rotate_file() error {
	 _, err := os.Stat(self.filename)
    if nil != err { // file exists
    	if os.IsNotExist(err) {
    		return nil
    	}
        return err
    }

    fname2 := self.filename + fmt.Sprintf(".%04d", self.maxNum)
    _, err = os.Stat(fname2)
    if nil == err {
        err = os.Remove(fname2)
        if err != nil {
             return err
        }
    }

    fname1 := fname2
    for num := self.maxNum - 1; num > 0; num-- {
        fname2 = fname1
        fname1 = self.filename + fmt.Sprintf(".%04d", num)

        _, err = os.Stat(fname1)
        if nil != err {
            continue
        }
        err = os.Rename(fname1, fname2)
        if err != nil {
            return err
        }
    }

    err = os.Rename(self.filename, fname1)
    if err != nil {
        return err
    }
}

func (self *ShellJob) do_run() {
	out, e := os.OpenFile(self.logfile, os.O_APPEND|os.O_CREATE, 0)
	if nil != e {
		log.Println("["+self.name+"] open log file("+self.logfile+") failed,", e)
		return
	}
	defer out.Close()
	io.WriteString(out, "=============== begin ===============\r\n")
	defer io.WriteString(out, "===============  end  ===============\r\n")

	cmd := exec.Command(self.execute, self.arguments...)
	cmd.Stderr = out
	cmd.Stdout = out
	if nil != self.environments && 0 != len(self.environments) {
		os_env := os.Environ()
		environments := make([]string, 0, len(self.arguments)+len(os_env))
		environments = append(environments, os_env...)
		environments = append(environments, self.environments...)
		cmd.Env = environments
	}

	io.WriteString(out, cmd.Path)
	for idx, s := range cmd.Args {
		if 0 == idx {
			continue
		}
		io.WriteString(out, "\r\n \t\t")
		io.WriteString(out, s)
	}
	io.WriteString(out, "\r\n===============  out  ===============\r\n")

	if e = cmd.Start(); nil != e {
		io.WriteString(out, "start failed, "+e.Error()+"\r\n")
		return
	}
	c := make(chan error, 10)
	go func() {
		c <- cmd.Wait()
	}()

	select {
	case e := <-c:
		out.Seek(0, os.SEEK_END)
		if nil != e {
			io.WriteString(out, "run failed, "+e.Error()+"\r\n")
		} else if nil != cmd.ProcessState {
			io.WriteString(out, "run ok, exit with "+cmd.ProcessState.String()+".\r\n")
		}
	case <-time.After(self.timeout):
		killByPid(cmd.Process.Pid)
		out.Seek(0, os.SEEK_END)
		io.WriteString(out, "run timeout, kill it.\r\n")
		log.Println("[" + self.name + "] run timeout, kill it.")
	}
}
