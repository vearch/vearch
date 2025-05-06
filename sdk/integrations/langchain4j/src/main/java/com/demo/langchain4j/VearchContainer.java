package com.demo.langchain4j;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

public class VearchContainer extends GenericContainer<VearchContainer> {

    public VearchContainer() {
        super("vearch/vearch:latest");
        withExposedPorts(9001, 8817);
        withCommand("all");
        withCopyFileToContainer(MountableFile.forClasspathResource("config.toml"), "/vearch/config.toml");
        waitingFor(Wait.forLogMessage(".*INFO : server pid:1.*\\n", 1));
    }
}
