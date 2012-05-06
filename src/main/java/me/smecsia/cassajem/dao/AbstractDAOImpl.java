package me.smecsia.cassajem.dao;

import me.smecsia.cassajem.EntityManagerFactory;
import me.smecsia.cassajem.api.BasicEntity;

public class AbstractDAOImpl<T extends BasicEntity> extends AbstractDAO<T>{

    protected AbstractDAOImpl(EntityManagerFactory entityManagerFactory, DAOFactory daoFactory, Class<? extends  BasicEntity> entityClass) {
        super(entityManagerFactory, daoFactory, entityClass);
    }

}
