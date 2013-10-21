package main

import (
	"io"
	"log"
	"os"
	"os/exec"
	"time"
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
}

func (self *ShellJob) Run() {
	out, e := os.OpenFile(self.logfile, os.O_APPEND|os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0)
	if nil != e {
		log.Println("["+self.name+"] open log file("+self.logfile+") failed,", e)
		return
	}
	defer out.Close()
	io.WriteString(out, "["+self.name+"] =============== begin ===============")
	defer io.WriteString(out, "["+self.name+"] ===============  end  ===============")

	cmd := exec.Command(self.execute, self.arguments...)
	cmd.Stderr = out
	cmd.Stdin = out
	if nil != self.environments && 0 != len(self.environments) {
		os_env := os.Environ()
		environments := make([]string, 0, len(self.arguments)+len(os_env))
		environments = append(environments, os_env...)
		environments = append(environments, self.environments...)
		cmd.Env = environments
	}

	if e = cmd.Start(); nil != e {
		io.WriteString(out, "["+self.name+"] start failed, "+e.Error())
		return
	}
	c := make(chan error, 10)
	go func() {
		c <- cmd.Wait()
	}()

	select {
	case e := <-c:
		if nil != e {
			io.WriteString(out, "["+self.name+"] run failed, "+e.Error())
		}
	case <-time.After(self.timeout):
		killByPid(cmd.Process.Pid)
		io.WriteString(out, "["+self.name+"] run timeout, kill it")
	}
}
