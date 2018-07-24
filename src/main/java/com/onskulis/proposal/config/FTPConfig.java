package com.onskulis.proposal.config;

import com.jcraft.jsch.ChannelSftp;
import com.onskulis.proposal.business.Parser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.sftp.filters.SftpSimplePatternFileListFilter;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizer;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizingMessageSource;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import java.io.File;

/**
 * FTP Connection configuration class which contains all the credentials,
 * as well as Spring Integration Beans for downloading the files
 */
@Configuration
public class FTPConfig {

    @Value("${ftp.host:localhost}")
    private String HOST;
    @Value("${ftp.port:22}")
    private Integer PORT;
    @Value("${ftp.user:tester}")
    private String USER;
    @Value("${ftp.password:password}")
    private String PASSWORD;
    @Value("${ftp.remote_dir:/}")
    private String REMOTE_DIR;
    private String LOCAL_DIR = "ftp-local";
    @Value("${ftp.file_type:csv}")
    private String FILE_TYPE;


    /**
     * Creates a ftp session factory for retrieving sessions when needed
     *
     * @return SessionFactory
     */
    @Bean
    public SessionFactory<ChannelSftp.LsEntry> sftpSessionFactory() {

        DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);
        factory.setHost(this.HOST);
        factory.setPort(this.PORT);
        factory.setUser(this.USER);
        factory.setPassword(this.PASSWORD);
        factory.setAllowUnknownKeys(true);

        return new CachingSessionFactory<ChannelSftp.LsEntry>(factory);
    }

    /**
     * TODO
     *
     * @return SftpInboundFileSynchronizer
     */
    @Bean
    public SftpInboundFileSynchronizer sftpInboundFileSynchronizer() {

        SftpInboundFileSynchronizer fileSynchronizer = new SftpInboundFileSynchronizer(sftpSessionFactory());
        fileSynchronizer.setDeleteRemoteFiles(false);
        fileSynchronizer.setRemoteDirectory(this.REMOTE_DIR);
        fileSynchronizer.setFilter(new SftpSimplePatternFileListFilter("*."+this.FILE_TYPE));

        return fileSynchronizer;
    }

    /**
     * TODO
     *
     * @return
     */
    @Bean
    @InboundChannelAdapter(channel = "sftpChannel", poller = @Poller(fixedDelay = "5000"))
    public MessageSource<File> sftpMessageSource() {

        SftpInboundFileSynchronizingMessageSource source = new SftpInboundFileSynchronizingMessageSource(
                sftpInboundFileSynchronizer());
        source.setLocalDirectory(new File(this.LOCAL_DIR));
        source.setAutoCreateLocalDirectory(true);
        source.setLocalFilter(new AcceptOnceFileListFilter<File>());

        return source;
    }

    /**
     * TODO
     *
     * @return
     */
    @Bean
    @ServiceActivator(inputChannel = "sftpChannel")
    public MessageHandler handler() {

        Parser parser = new Parser();

        return new MessageHandler() {

            @Override
            public void handleMessage(Message<?> message) throws MessagingException {

                if(message.getPayload() instanceof File) {
                    File f = (File) message.getPayload();
                    try {
                        //TODO: move file over pipeline to inbound adapter, parser in particular
                        parser.parseFilesToPOJOs(f.getAbsolutePath());
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }
            }

        };
    }
}
